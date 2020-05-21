/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef RAX_CHUNK_H
#define RAX_CHUNK_H

#include "consts.h"
#include "generic_chunk.h"
#include "redismodule.h"
#include <sys/types.h>

typedef struct RaxChunk {
  short max_samples;
  RedisModuleDict *samples;
  RedisModuleDictIter *dictIter;
} RaxChunk;

typedef struct RaxChunkIterator {
  RaxChunk *chunk;
  void *(*Next)(RedisModuleDictIter *di, size_t *keylen, void **dataptr);
} RaxChunkIterator;

Chunk_t *RaxC_NewChunk(size_t sampleCount);
void RaxC_FreeChunk(Chunk_t *chunk);
size_t RaxC_GetChunkSize(Chunk_t *chunk);

ChunkResult RaxC_AddSample(Chunk_t *chunk, Sample *sample);
ChunkResult RaxC_RemoveSample(Chunk_t *chunk, Sample *sample);

u_int64_t RaxC_NumOfSample(Chunk_t *chunk);
timestamp_t RaxC_GetLastTimestamp(Chunk_t *chunk);
timestamp_t RaxC_GetFirstTimestamp(Chunk_t *chunk);

ChunkIter_t *RaxC_NewChunkIterator(Chunk_t *chunk, bool rev);
ChunkResult RaxC_ChunkIteratorGetNext(ChunkIter_t *iterator, Sample *sample);
ChunkResult RaxC_ChunkIteratorGetPrev(ChunkIter_t *iterator, Sample *sample);
void RaxC_FreeChunkIterator(ChunkIter_t *iter, bool freeChunk);

#endif

//RedisModule_CreateDict(NULL)