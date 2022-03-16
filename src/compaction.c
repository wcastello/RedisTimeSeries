/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "compaction.h"

#include "load_io_error_macros.h"
#include "rdb.h"

#include "rmutil/alloc.h"

#include <ctype.h>
#include <float.h>
#include <math.h> // sqrt
#include <string.h>

#ifdef _DEBUG
#include "valgrind/valgrind.h"
#endif

typedef struct MaxMinContext
{
    double minValue;
    double maxValue;
    char isResetted;
} MaxMinContext;

typedef struct SingleValueContext
{
    double value;
    char isResetted;
} SingleValueContext;

typedef struct AvgContext
{
    double val;
    double cnt;
    bool isOverflow;
} AvgContext;

typedef struct WeightData
{
    double weightSum;
    timestamp_t prevPrevTS;
    timestamp_t prevTS;
    double prevValue;
    timestamp_t bucketStartTS;
    timestamp_t bucketEndTS;
    bool is_first_bucket;
    bool is_last_ts_handled;
    int64_t weight_cnt;
    int64_t weight_sum;
} WeightData;

typedef struct WAvgContext
{
    AvgContext avgContext;
    WeightData weightData;
} WAvgContext;

typedef struct StdContext
{
    double sum;
    double sum_2; // sum of (values^2)
    u_int64_t cnt;
} StdContext;

void *SingleValueCreateContext() {
    SingleValueContext *context = (SingleValueContext *)malloc(sizeof(SingleValueContext));
    context->value = 0;
    context->isResetted = TRUE;
    return context;
}

void SingleValueReset(void *contextPtr) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = 0;
    context->isResetted = TRUE;
}

int SingleValueFinalize(void *contextPtr, double *val) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    if (context->isResetted == true) {
        return TSDB_ERROR;
    }
    *val = context->value;
    return TSDB_OK;
}

void SingleValueWriteContext(void *contextPtr, RedisModuleIO *io) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    RedisModule_SaveDouble(io, context->value);
    RedisModule_SaveUnsigned(io, context->isResetted);
}

int SingleValueReadContext(void *contextPtr, RedisModuleIO *io, int encver) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = LoadDouble_IOError(io, goto err);
    if (encver >= TS_IS_RESSETED_DUP_POLICY_RDB_VER) {
        context->isResetted = LoadUnsigned_IOError(io, goto err);
    }
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

static inline void _AvgInitContext(AvgContext *context) {
    context->cnt = 0;
    context->val = 0;
    context->isOverflow = false;
}

void *AvgCreateContext() {
    AvgContext *context = (AvgContext *)malloc(sizeof(AvgContext));
    _AvgInitContext(context);
    return context;
}

// Except valgrind it's equivalent to sizeof(long double) > 8
#if !defined(_DEBUG) && !defined(_VALGRIND)
bool hasLongDouble = sizeof(long double) > 8;
#else
bool hasLongDouble = false;
#endif

void AvgAddValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    AvgContext *context = (AvgContext *)contextPtr;
    context->cnt++;

    // Test for overflow
    if (unlikely(((context->val < 0.0) == (value < 0.0) &&
                  (fabs(context->val) > (DBL_MAX - fabs(value)))) ||
                 context->isOverflow)) {
        // calculating: avg(t+1) = t*avg(t)/(t+1) + val/(t+1)

        long double ld_val = context->val;
        long double ld_value = value;
        if (likely(hasLongDouble)) { // better accuracy
            ld_val /= context->cnt;
            if (context->isOverflow) {
                ld_val *= (long double)(context->cnt - 1);
            }
        } else {
            if (context->isOverflow) {
                ld_val *= ((long double)(context->cnt - 1) / context->cnt);
            } else {
                ld_val /= context->cnt;
            }
        }
        ld_val += (ld_value / (long double)context->cnt);
        context->val = ld_val;
        context->isOverflow = true;
    } else { // No Overflow
        context->val += value;
    }
}

int AvgFinalize(void *contextPtr, double *value) {
    AvgContext *context = (AvgContext *)contextPtr;
    if (context->cnt == 0)
        return TSDB_ERROR;
    if (unlikely(context->isOverflow)) {
        *value = context->val;
    } else {
        *value = context->val / context->cnt;
    }
    return TSDB_OK;
}

void AvgReset(void *contextPtr) {
    _AvgInitContext(contextPtr);
}

void AvgWriteContext(void *contextPtr, RedisModuleIO *io) {
    AvgContext *context = (AvgContext *)contextPtr;
    RedisModule_SaveDouble(io, context->val);
    RedisModule_SaveDouble(io, context->cnt);
    RedisModule_SaveUnsigned(io, context->isOverflow);
}

int AvgReadContext(void *contextPtr, RedisModuleIO *io, int encver) {
    AvgContext *context = (AvgContext *)contextPtr;
    context->val = LoadDouble_IOError(io, goto err);
    context->cnt = LoadDouble_IOError(io, goto err);
    context->isOverflow = false;
    if (encver >= TS_OVERFLOW_RDB_VER) {
        context->isOverflow = !!(LoadUnsigned_IOError(io, goto err));
    }
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

static inline void _WAvginitContext(WAvgContext *context) {
    _AvgInitContext(&context->avgContext);
    context->weightData.weightSum = 0;
    context->weightData.prevPrevTS;
    context->weightData.prevTS = DC;    // Just an arbitrary value
    context->weightData.prevValue = DC; // Just an arbitrary value
    context->weightData.bucketStartTS;
    context->weightData.bucketEndTS;
    context->weightData.is_first_bucket = true;
    context->weightData.is_last_ts_handled = false;
    context->weightData.weight_cnt = 0;
    context->weightData.weight_sum = 0;
}

void *WAvgCreateContext() {
    WAvgContext *context = (WAvgContext *)malloc(sizeof(WAvgContext));
    _WAvginitContext(context);
    return context;
}

static inline void _update_wavgContext(WAvgContext *wcontext, const double *value, const timestamp_t *ts) {
    wcontext->weightData.prevPrevTS = wcontext->weightData.prevTS;
    wcontext->weightData.prevValue = *value;
    wcontext->weightData.prevTS = *ts;
}

void WAvgAddBucketParams(void *contextPtr, timestamp_t bucketStartTS, timestamp_t bucketEndTS) {
    WAvgContext *context = (WAvgContext *)contextPtr;
    context->weightData.bucketStartTS = bucketStartTS;
    context->weightData.bucketEndTS = bucketEndTS;
}

void WAvgAddPrevBucketLastSample(void *contextPtr, double value, timestamp_t ts) {
    WAvgContext *wcontext = (WAvgContext *)contextPtr;
    _update_wavgContext(&value, &ts);
    wcontext->weightData.is_first_bucket = false;
}

void WAvgAddValue(void *contextPtr, double value, timestamp_t ts) {
    WAvgContext *wcontext = (WAvgContext *)contextPtr;
    AvgContext *context = &wcontext->avgContext;
    const int64_t *weightCnt = &wcontext->weightData.weight_cnt;
    ++(*weightCnt);
    const timestamp_t *prevTS = wcontext->weightData.prevTS;
    const double *prev_value = wcontext->weightData.prevValue;
    const timestamp_t time_delta = ts - (*prevTS);
    const double half_time_delta = time_delta/2.0;
    const bool *is_first_bucket = &wcontext->weightData.is_first_bucket;
    const timestamp_t *startTS =  &wcontext->weightData.bucketStartTS;
    double weight;

    // add prev value with it's 2nd weight and add current value with it's first weight

    if((*weightCnt) == 1) { // First sample in bucket
        if(!(*is_first_bucket)) {
            if((*prevTS) + half_time_delta <= (*startTS)) {
                // prev_ts --- --- half_way --- bucket_start --- fisrt_ts
                weight = (ts - (*startTS));
                context->weightData.weight_sum += weight;
                AvgAddValue(context, value*weight, DC);
            } else {
                // prev_ts --- bucket_start --- half_way --- --- fisrt_ts
                context->weightData.weight_sum += half_time_delta;
                AvgAddValue(context, value*half_time_delta, DC);
                ++(*weightCnt); // inc the cnt one more time for the 2nd weight
                weight = (*prevTS) + half_time_delta - (*startTS);
                context->weightData.weight_sum += weight;
                AvgAddValue(context, (*prev_value)*weight, DC);
            }
        }
        // else: cur sample is the first in the series,
        // assume the delta from the prev sample is same as the delta from next sample
        // will be handled on next sample

        _update_wavgContext(wcontext, &value, &ts);
        return;
    } else if(unlikely((*weightCnt) == 2 && (*is_first_bucket))) {
        // 2nd sample in bucket and first bucket in the series
        // extrapolate the weight of the 1st sample to be time_delta
        // weightCnt already incremented on prev sample
        double weight = min(half_time_delta, (*prevTS) - (*startTS)) + half_time_delta;
        context->weightData.weight_sum += weight;
        AvgAddValue(context, (*prev_value)*weight, DC);
    } else {
        ++(*weightCnt);
        context->weightData.weight_sum += half_time_delta;
        // add prev value with it's 2nd weight
        AvgAddValue(context, (*prev_value)*half_time_delta, DC);
    }

    // add cur value with it's 1st weight
    // wt = (t(k) - t(k - 1))/2)
    context->weightData.weight_sum += half_time_delta;
    AvgAddValue(context, value*half_time_delta, 0);

    _update_wavgContext(wcontext, &value, &ts);
}

void WAvgAddNextBucketFirstSample(void *contextPtr, double value, timestamp_t ts) {
    WAvgContext *wcontext = (WAvgContext *)contextPtr;
    AvgContext *context = &wcontext->avgContext;
    const int64_t *weightCnt = &wcontext->weightData.weight_cnt;
    ++(*weightCnt);
    const timestamp_t *prevTS = wcontext->weightData.prevTS;
    const double *prev_value = wcontext->weightData.prevValue;
    const timestamp_t time_delta = ts - (*prevTS);
    const double half_time_delta = time_delta/2.0;
    const bool *is_first_bucket = &wcontext->weightData.is_first_bucket;
    const timestamp_t *startTS =  &wcontext->weightData.bucketStartTS;
    const timestamp_t *endTS =  &wcontext->weightData.bucketEndTS;
    double weight;

    if(unlikely((*weightCnt) == 2 && (*is_first_bucket))) {
        // Only 1 sample in bucket and first in the series
        // extrapolate the 1st weight of the 1st sample to be time_delta
        // weightCnt already incremented on prev sample
        weight = min(half_time_delta, (*prevTS) - (*startTS));
        context->weightData.weight_sum += weight;
        AvgAddValue(context, (*prev_value)*weight, DC);
    }

    // add the 2nd weight of prev sample
    if((*prevTS) + half_time_delta >= (*endTS)) {
        // last_ts --- bucket_end --- half_way --- --- next_ts
        weight = (*endTS) - (*prevTS);
        context->weightData.weight_sum += weight;
        AvgAddValue(context, prev_value*weight, DC);
    } else {
        // last_ts --- --- half_way --- bucket_end --- next_ts
        context->weightData.weight_sum += half_time_delta;
        AvgAddValue(context, prev_value*half_time_delta, DC);
        ++(*weightCnt);
        weight = (*endTS) - ((*prevTS) + half_time_delta);
        context->weightData.weight_sum += weight;
        AvgAddValue(context, value*weight, DC);
    }

    context->weightData.is_last_ts_handled = true;
}

int WAvgFinalize(void *contextPtr, double *value) {
    WAvgContext *wcontext = (WAvgContext *)contextPtr;
    AvgContext *context = &wcontext->avgContext;
    const int64_t *weightCnt = &wcontext->weightData.weight_cnt;

    if(!context->weightData.is_last_ts_handled) {
        ++(*weightCnt);
        if(unlikely((*weightCnt) == 2 && (*is_first_bucket))) {
            // Only 1 sample in bucket and that's the only sample in the series
            // don't use weights at all
            (*weightCnt) = 0;
            context->weightData.weight_sum = 0;
            AvgAddValue(context, wcontext->weightData.prevValue, DC);
        } else {
            // This is the last bucket in the series
            // extrapolate the weight of the prev sample to be the prev time_delta
            const timestamp_t *prevTS = wcontext->weightData.prevTS;
            const timestamp_t *endTS =  &wcontext->weightData.bucketEndTS;
            const timestamp_t time_delta = (*prevTS) - wcontext->weightData.prevPrevTS;
            const double half_time_delta = time_delta/2.0;
            double weight = min(half_time_delta, (*endTS) - (*prevTS));
            context->weightData.weight_sum += weight;
            AvgAddValue(context, wcontext->weightData.prevValue*weight, DC);
        }
    }

    if(*weightCnt > 0) {
        // Normalizing the weighting for each time by dividing each weight by the mean of all weights 
        const double avg_weight = context->weightData.weight_sum/context->weightData.weight_cnt;
        context->val /= avg_weight;
    }

    return AvgFinalize(context, value);
}

void WAvgReset(void *contextPtr, timestamp_t startTS, timestamp_t endTS) {
    _WAvginitContext(contextPtr, &startTS, &endTS);
}

void WAvgWriteContext(void *contextPtr, RedisModuleIO *io) {
    WAvgContext *context = (WAvgContext *)contextPtr;
    AvgWriteContext(&wcontext->avgContext, io);
    RedisModule_SaveDouble(io, context->weightData.weightSum);
    RedisModule_SaveUnsigned(io, context->weightData.prevPrevTS);
    RedisModule_SaveUnsigned(io, context->weightData.prevTS);
    RedisModule_SaveDouble(io, context->weightData.prevValue);
    RedisModule_SaveUnsigned(io, context->weightData.bucketStartTS);
    RedisModule_SaveUnsigned(io, context->weightData.bucketEndTS);
    RedisModule_SaveUnsigned(io, context->weightData.is_first_bucket);
    RedisModule_SaveUnsigned(io, context->weightData.is_last_ts_handled);
    RedisModule_SaveUnsigned(io, context->weightData.weight_cnt);
    RedisModule_SaveUnsigned(io, context->weightData.weight_sum);    
}

int WAvgReadContext(void *contextPtr, RedisModuleIO *io, int encver) {
    AvgContext *context = (AvgContext *)contextPtr;
    if(AvgReadContext(&wcontext->avgContext, io, encver) == TSDB_ERROR) {
        goto err;
    }

    context->weightSum = LoadDouble_IOError(io, goto err);
    context->prevPrevTS = LoadUnsigned_IOError(io, goto err);
    context->prevTS = LoadUnsigned_IOError(io, goto err);
    context->prevValue = LoadDouble_IOError(io, goto err);
    context->bucketStartTS = LoadUnsigned_IOError(io, goto err);
    context->bucketEndTS = LoadUnsigned_IOError(io, goto err);
    context->is_first_bucket = LoadUnsigned_IOError(io, goto err);
    context->is_last_ts_handled = LoadUnsigned_IOError(io, goto err);
    context->weight_cnt = LoadUnsigned_IOError(io, goto err);
    context->weight_sum = LoadUnsigned_IOError(io, goto err);
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

void *StdCreateContext() {
    StdContext *context = (StdContext *)malloc(sizeof(StdContext));
    context->cnt = 0;
    context->sum = 0;
    context->sum_2 = 0;
    return context;
}

void StdAddValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    StdContext *context = (StdContext *)contextPtr;
    ++context->cnt;
    context->sum += value;
    context->sum_2 += value * value;
}

static inline double variance(double sum, double sum_2, double count) {
    if (count == 0) {
        return 0;
    }

    /*  var(X) = sum((x_i - E[X])^2)
     *  = sum(x_i^2) - 2 * sum(x_i) * E[X] + E^2[X] */
    return (sum_2 - 2 * sum * sum / count + pow(sum / count, 2) * count) / count;
}

int VarPopulationFinalize(void *contextPtr, double *value) {
    StdContext *context = (StdContext *)contextPtr;
    uint64_t count = context->cnt;
    if (count == 0) {
        return TSDB_ERROR;
    }
    *value = variance(context->sum, context->sum_2, count);
    return TSDB_OK;
}

int VarSamplesFinalize(void *contextPtr, double *value) {
    StdContext *context = (StdContext *)contextPtr;
    uint64_t count = context->cnt;
    if (count == 0) {
        return TSDB_ERROR;
    } else if (count == 1) {
        *value = 0;
    } else {
        *value = variance(context->sum, context->sum_2, count) * count / (count - 1);
    }
    return TSDB_OK;
}

int StdPopulationFinalize(void *contextPtr, double *value) {
    double val;
    int rv = VarPopulationFinalize(contextPtr, &val);
    if (rv != TSDB_OK) {
        return rv;
    }
    *value = sqrt(val);
    return TSDB_OK;
}

int StdSamplesFinalize(void *contextPtr, double *value) {
    double val;
    int rv = VarSamplesFinalize(contextPtr, &val);
    if (rv != TSDB_OK) {
        return rv;
    }
    *value = sqrt(val);
    return TSDB_OK;
}

void StdReset(void *contextPtr) {
    StdContext *context = (StdContext *)contextPtr;
    context->cnt = 0;
    context->sum = 0;
    context->sum_2 = 0;
}

void StdWriteContext(void *contextPtr, RedisModuleIO *io) {
    StdContext *context = (StdContext *)contextPtr;
    RedisModule_SaveDouble(io, context->sum);
    RedisModule_SaveDouble(io, context->sum_2);
    RedisModule_SaveUnsigned(io, context->cnt);
}

int StdReadContext(void *contextPtr, RedisModuleIO *io, REDISMODULE_ATTR_UNUSED int encver) {
    StdContext *context = (StdContext *)contextPtr;
    context->sum = LoadDouble_IOError(io, goto err);
    context->sum_2 = LoadDouble_IOError(io, goto err);
    context->cnt = LoadUnsigned_IOError(io, goto err);
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

void rm_free(void *ptr) {
    free(ptr);
}

static AggregationClass waggAvg = { .createContext = WAvgCreateContext,
                                   .appendValue = WAvgAddValue,
                                   .freeContext = rm_free,
                                   .finalize = WAvgFinalize,
                                   .writeContext = WAvgWriteContext,
                                   .readContext = WAvgReadContext,
                                   .addBucketParams = WAvgAddBucketParams,
                                   .addPrevBucketLastSample = WAvgAddPrevBucketLastSample,
                                   .addNextBucketFirstSample = WAvgAddNextBucketFirstSample,
                                   .resetContext = WAvgReset };

static AggregationClass aggAvg = { .createContext = AvgCreateContext,
                                   .appendValue = AvgAddValue,
                                   .freeContext = rm_free,
                                   .finalize = AvgFinalize,
                                   .writeContext = AvgWriteContext,
                                   .readContext = AvgReadContext,
                                   .addBucketParams = NULL,
                                   .addPrevBucketLastSample = NULL,
                                   .addNextBucketFirstSample = NULL,
                                   .resetContext = AvgReset };

static AggregationClass aggStdP = { .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .freeContext = rm_free,
                                    .finalize = StdPopulationFinalize,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .resetContext = StdReset };

static AggregationClass aggStdS = { .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .freeContext = rm_free,
                                    .finalize = StdSamplesFinalize,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .resetContext = StdReset };

static AggregationClass aggVarP = { .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .freeContext = rm_free,
                                    .finalize = VarPopulationFinalize,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .resetContext = StdReset };

static AggregationClass aggVarS = { .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .freeContext = rm_free,
                                    .finalize = VarSamplesFinalize,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .resetContext = StdReset };

void *MaxMinCreateContext() {
    MaxMinContext *context = (MaxMinContext *)malloc(sizeof(MaxMinContext));
    context->minValue = 0;
    context->maxValue = 0;
    context->isResetted = TRUE;
    return context;
}

void MaxMinAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->maxValue = value;
        context->minValue = value;
    } else {
        if (value > context->maxValue) {
            context->maxValue = value;
        }
        if (value < context->minValue) {
            context->minValue = value;
        }
    }
}

int MaxFinalize(void *contextPtr, double *value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (context->isResetted == TRUE) {
        return TSDB_ERROR;
    }
    *value = context->maxValue;
    return TSDB_OK;
}

int MinFinalize(void *contextPtr, double *value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (context->isResetted == TRUE) {
        return TSDB_ERROR;
    }
    *value = context->minValue;
    return TSDB_OK;
}

int RangeFinalize(void *contextPtr, double *value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (context->isResetted == TRUE) {
        return TSDB_ERROR;
    }
    *value = context->maxValue - context->minValue;
    return TSDB_OK;
}

void MaxMinReset(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->maxValue = 0;
    context->minValue = 0;
    context->isResetted = TRUE;
}

void MaxMinWriteContext(void *contextPtr, RedisModuleIO *io) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    RedisModule_SaveDouble(io, context->maxValue);
    RedisModule_SaveDouble(io, context->minValue);
    RedisModule_SaveStringBuffer(io, &context->isResetted, 1);
}

int MaxMinReadContext(void *contextPtr, RedisModuleIO *io, REDISMODULE_ATTR_UNUSED int encver) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    char *sb = NULL;
    size_t len = 1;
    context->maxValue = LoadDouble_IOError(io, goto err);
    context->minValue = LoadDouble_IOError(io, goto err);
    sb = LoadStringBuffer_IOError(io, &len, goto err);
    context->isResetted = sb[0];
    RedisModule_Free(sb);
    return TSDB_OK;

err:
    if (sb) {
        RedisModule_Free(sb);
    }
    return TSDB_ERROR;
}

void SumAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value += value;
    context->isResetted = FALSE;
}

void CountAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value++;
    context->isResetted = FALSE;
}

int CountFinalize(void *contextPtr, double *val) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    *val = context->value;
    return TSDB_OK;
}

void FirstAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->value = value;
    }
}

void LastAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = value;
    context->isResetted = FALSE;
}

static AggregationClass aggMax = { .createContext = MaxMinCreateContext,
                                   .appendValue = MaxMinAppendValue,
                                   .freeContext = rm_free,
                                   .finalize = MaxFinalize,
                                   .writeContext = MaxMinWriteContext,
                                   .readContext = MaxMinReadContext,
                                   .addBucketParams = NULL,
                                   .addPrevBucketLastSample = NULL,
                                   .addNextBucketFirstSample = NULL,
                                   .resetContext = MaxMinReset };

static AggregationClass aggMin = { .createContext = MaxMinCreateContext,
                                   .appendValue = MaxMinAppendValue,
                                   .freeContext = rm_free,
                                   .finalize = MinFinalize,
                                   .writeContext = MaxMinWriteContext,
                                   .readContext = MaxMinReadContext,
                                   .addBucketParams = NULL,
                                   .addPrevBucketLastSample = NULL,
                                   .addNextBucketFirstSample = NULL,
                                   .resetContext = MaxMinReset };

static AggregationClass aggSum = { .createContext = SingleValueCreateContext,
                                   .appendValue = SumAppendValue,
                                   .freeContext = rm_free,
                                   .finalize = SingleValueFinalize,
                                   .writeContext = SingleValueWriteContext,
                                   .readContext = SingleValueReadContext,
                                   .addBucketParams = NULL,
                                   .addPrevBucketLastSample = NULL,
                                   .addNextBucketFirstSample = NULL,
                                   .resetContext = SingleValueReset };

static AggregationClass aggCount = { .createContext = SingleValueCreateContext,
                                     .appendValue = CountAppendValue,
                                     .freeContext = rm_free,
                                     .finalize = CountFinalize,
                                     .writeContext = SingleValueWriteContext,
                                     .readContext = SingleValueReadContext,
                                     .addBucketParams = NULL,
                                     .addPrevBucketLastSample = NULL,
                                     .addNextBucketFirstSample = NULL,
                                     .resetContext = SingleValueReset };

static AggregationClass aggFirst = { .createContext = SingleValueCreateContext,
                                     .appendValue = FirstAppendValue,
                                     .freeContext = rm_free,
                                     .finalize = SingleValueFinalize,
                                     .writeContext = SingleValueWriteContext,
                                     .readContext = SingleValueReadContext,
                                     .addBucketParams = NULL,
                                     .addPrevBucketLastSample = NULL,
                                     .addNextBucketFirstSample = NULL,
                                     .resetContext = SingleValueReset };

static AggregationClass aggLast = { .createContext = SingleValueCreateContext,
                                    .appendValue = LastAppendValue,
                                    .freeContext = rm_free,
                                    .finalize = SingleValueFinalize,
                                    .writeContext = SingleValueWriteContext,
                                    .readContext = SingleValueReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .resetContext = SingleValueReset };

static AggregationClass aggRange = { .createContext = MaxMinCreateContext,
                                     .appendValue = MaxMinAppendValue,
                                     .freeContext = rm_free,
                                     .finalize = RangeFinalize,
                                     .writeContext = MaxMinWriteContext,
                                     .readContext = MaxMinReadContext,
                                     .addBucketParams = NULL,
                                     .addPrevBucketLastSample = NULL,
                                     .addNextBucketFirstSample = NULL,
                                     .resetContext = MaxMinReset };

int StringAggTypeToEnum(const char *agg_type) {
    return StringLenAggTypeToEnum(agg_type, strlen(agg_type));
}

int RMStringLenAggTypeToEnum(RedisModuleString *aggTypeStr) {
    size_t str_len;
    const char *aggTypeCStr = RedisModule_StringPtrLen(aggTypeStr, &str_len);
    return StringLenAggTypeToEnum(aggTypeCStr, str_len);
}

int StringLenAggTypeToEnum(const char *agg_type, size_t len) {
    int result = TS_AGG_INVALID;
    char agg_type_lower[len];
    for (int i = 0; i < len; i++) {
        agg_type_lower[i] = tolower(agg_type[i]);
    }
    if (len == 3) {
        if (strncmp(agg_type_lower, "min", len) == 0 && len == 3) {
            result = TS_AGG_MIN;
        } else if (strncmp(agg_type_lower, "max", len) == 0) {
            result = TS_AGG_MAX;
        } else if (strncmp(agg_type_lower, "sum", len) == 0) {
            result = TS_AGG_SUM;
        } else if (strncmp(agg_type_lower, "avg", len) == 0) {
            result = TS_AGG_AVG;
        }
    } else if (len == 4) {
        if (strncmp(agg_type_lower, "last", len) == 0) {
            result = TS_AGG_LAST;
        }
    } else if (len == 5) {
        if (strncmp(agg_type_lower, "count", len) == 0) {
            result = TS_AGG_COUNT;
        } else if (strncmp(agg_type_lower, "range", len) == 0) {
            result = TS_AGG_RANGE;
        } else if (strncmp(agg_type_lower, "first", len) == 0) {
            result = TS_AGG_FIRST;
        } else if (strncmp(agg_type_lower, "std.p", len) == 0) {
            result = TS_AGG_STD_P;
        } else if (strncmp(agg_type_lower, "std.s", len) == 0) {
            result = TS_AGG_STD_S;
        } else if (strncmp(agg_type_lower, "var.p", len) == 0) {
            result = TS_AGG_VAR_P;
        } else if (strncmp(agg_type_lower, "var.s", len) == 0) {
            result = TS_AGG_VAR_S;
        }
    }
    return result;
}

const char *AggTypeEnumToString(TS_AGG_TYPES_T aggType) {
    switch (aggType) {
        case TS_AGG_MIN:
            return "MIN";
        case TS_AGG_MAX:
            return "MAX";
        case TS_AGG_SUM:
            return "SUM";
        case TS_AGG_AVG:
            return "AVG";
        case TS_AGG_STD_P:
            return "STD.P";
        case TS_AGG_STD_S:
            return "STD.S";
        case TS_AGG_VAR_P:
            return "VAR.P";
        case TS_AGG_VAR_S:
            return "VAR.S";
        case TS_AGG_COUNT:
            return "COUNT";
        case TS_AGG_FIRST:
            return "FIRST";
        case TS_AGG_LAST:
            return "LAST";
        case TS_AGG_RANGE:
            return "RANGE";
        case TS_AGG_NONE:
        case TS_AGG_INVALID:
        case TS_AGG_TYPES_MAX:
            break;
    }
    return "Unknown";
}

AggregationClass *GetAggClass(TS_AGG_TYPES_T aggType) {
    switch (aggType) {
        case TS_AGG_MIN:
            return &aggMin;
        case TS_AGG_MAX:
            return &aggMax;
        case TS_AGG_AVG:
            return &aggAvg;
        case TS_AGG_STD_P:
            return &aggStdP;
        case TS_AGG_STD_S:
            return &aggStdS;
        case TS_AGG_VAR_P:
            return &aggVarP;
        case TS_AGG_VAR_S:
            return &aggVarS;
        case TS_AGG_SUM:
            return &aggSum;
        case TS_AGG_COUNT:
            return &aggCount;
        case TS_AGG_FIRST:
            return &aggFirst;
        case TS_AGG_LAST:
            return &aggLast;
        case TS_AGG_RANGE:
            return &aggRange;
        case TS_AGG_NONE:
        case TS_AGG_INVALID:
        case TS_AGG_TYPES_MAX:
            break;
    }
    return NULL;
}
