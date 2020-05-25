#include "rax_chunk.h"
#include "endianconv.h"   // htonu64
#include <string.h>       // memcpy
#include <assert.h>

// TODO: unite with copy at tsdb.c
static void seriesEncodeTimestamp(void *buf, timestamp_t timestamp) {
    uint64_t e;
    e = htonu64(timestamp);
    memcpy(buf, &e, sizeof(e));
}

Chunk_t *RaxC_NewChunk(size_t sampleCount) {
  RaxChunk *chunk = (RaxChunk *)calloc(1, sizeof(*chunk));
  chunk->max_samples = sampleCount;
  chunk->samples = RedisModule_CreateDict(RTS_GlobalRedisCtx);
  chunk->dictIter = RedisModule_DictIteratorStartC(chunk->samples, "^", NULL, 0);
  return (Chunk_t *)chunk;
}

void RaxC_FreeChunk(Chunk_t *chunk) {
  RaxChunk *raxC = (RaxChunk *)chunk;
  void *sample;
  RedisModule_DictIteratorReseekC(raxC->dictIter, "^", NULL, 0);
  while (RedisModule_DictNextC(raxC->dictIter, NULL, &sample)) {
    free(sample);
  }
  RedisModule_DictIteratorStop(raxC->dictIter);
  RedisModule_FreeDict(RTS_GlobalRedisCtx, raxC->samples);
}

size_t RaxC_GetChunkSize(Chunk_t *chunk) {
  RaxChunk *raxC = (RaxChunk *)chunk;
  return RedisModule_DictSize(raxC->samples) * (sizeof(Sample) + sizeof(Sample *)/* + redis */);
}

ChunkResult RaxC_AddSample(Chunk_t *chunk, Sample *sample) {
  RaxChunk *raxC = (RaxChunk *)chunk;
  if (RedisModule_DictSize(raxC->samples) == raxC->max_samples) {
    return CR_END;
  }

  Sample *raxSample = (Sample *)malloc(sizeof(*raxSample));
  *raxSample = *sample;
  if (RedisModule_DictSize(raxC->samples) == 0) {
    raxC->firstTS = sample->timestamp;
  }
  raxC->lastTS = sample->timestamp;

  timestamp_t rax_key;
  seriesEncodeTimestamp(&rax_key, sample->timestamp);
  if (RedisModule_DictSetC(raxC->samples, &rax_key, sizeof(rax_key), raxSample) != REDISMODULE_OK) {
    assert(0);
  }
  return CR_OK;
}

u_int64_t RaxC_NumOfSample(Chunk_t *chunk) {
  RaxChunk *raxC = (RaxChunk *)chunk;
  return RedisModule_DictSize(raxC->samples);
}

timestamp_t RaxC_GetLastTimestamp(Chunk_t *chunk) {
  RaxChunk *raxC = (RaxChunk *)chunk;
  return raxC->lastTS;
}

timestamp_t RaxC_GetFirstTimestamp(Chunk_t *chunk) {
  RaxChunk *raxC = (RaxChunk *)chunk;
  return raxC->firstTS;
}

ChunkIter_t *RaxC_NewChunkIterator(Chunk_t *chunk, bool rev) {
  RaxChunkIterator *iter = (RaxChunkIterator *)malloc(sizeof(*iter));
  iter->chunk = chunk;

  if (rev == false) {
    RedisModule_DictIteratorReseekC(iter->chunk->dictIter, "^", NULL, 0);
    iter->Next = RedisModule_DictNextC;
  } else {
    RedisModule_DictIteratorReseekC(iter->chunk->dictIter, "$", NULL, 0);
    iter->Next = RedisModule_DictPrevC;
  }
  return (ChunkIter_t *)iter;
}

ChunkResult RaxC_ChunkIteratorGetNext(ChunkIter_t *iterator, Sample *sample) {
  RaxChunkIterator *iter = (RaxChunkIterator *)iterator;
  Sample *dictSample;
  if (iter->Next(iter->chunk->dictIter, NULL, (void **)&dictSample) == NULL) {
    return CR_END;
  }
  *sample = *dictSample;
  return CR_OK;
}

ChunkResult RaxC_ChunkIteratorGetPrev(ChunkIter_t *iterator, Sample *sample) {
  return RaxC_ChunkIteratorGetNext(iterator, sample);
}

void RaxC_FreeChunkIterator(ChunkIter_t *iter, bool freeChunk) {
  (void)freeChunk;
  free(iter);
}