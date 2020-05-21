#include "generic_chunk.h"
#include "chunk.h"
#include "compressed_chunk.h"
#include "rax_chunk.h"
#include "rmutil/alloc.h"

static ChunkFuncs regChunk = {
    .NewChunk = Uncompressed_NewChunk,
    .FreeChunk = Uncompressed_FreeChunk,

    .AddSample = Uncompressed_AddSample,

    .NewChunkIterator = Uncompressed_NewChunkIterator,
    .FreeChunkIterator = Uncompressed_FreeChunkIterator,
    .ChunkIteratorGetNext = Uncompressed_ChunkIteratorGetNext,
    .ChunkIteratorGetPrev = Uncompressed_ChunkIteratorGetPrev,

    .GetChunkSize = Uncompressed_GetChunkSize,
    .GetNumOfSample = Uncompressed_NumOfSample,
    .GetLastTimestamp = Uncompressed_GetLastTimestamp,
    .GetFirstTimestamp = Uncompressed_GetFirstTimestamp
};

static ChunkFuncs comprChunk = {
    .NewChunk = Compressed_NewChunk,
    .FreeChunk = Compressed_FreeChunk,

    .AddSample = Compressed_AddSample,

    .NewChunkIterator = Compressed_NewChunkIterator,
    .FreeChunkIterator = Compressed_FreeChunkIterator,
    .ChunkIteratorGetNext = Compressed_ChunkIteratorGetNext,
    /*** Reverse iteration is on temporary decompressed chunk ***/
    .ChunkIteratorGetPrev = Uncompressed_ChunkIteratorGetPrev,

    .GetChunkSize = Compressed_GetChunkSize,
    .GetNumOfSample = Compressed_ChunkNumOfSample,
    .GetLastTimestamp = Compressed_GetLastTimestamp,
    .GetFirstTimestamp = Compressed_GetFirstTimestamp
};

static ChunkFuncs raxChunk = {
    .NewChunk = RaxC_NewChunk,
    .FreeChunk = RaxC_FreeChunk,

    .AddSample = RaxC_AddSample,

    .NewChunkIterator = RaxC_NewChunkIterator,
    .FreeChunkIterator = RaxC_FreeChunkIterator,
    .ChunkIteratorGetNext = RaxC_ChunkIteratorGetNext,
    /*** Reverse iteration is on temporary decompressed chunk ***/
    .ChunkIteratorGetPrev = RaxC_ChunkIteratorGetPrev,

    .GetChunkSize = RaxC_GetChunkSize,
    .GetNumOfSample = RaxC_NumOfSample,
    .GetLastTimestamp = RaxC_GetLastTimestamp,
    .GetFirstTimestamp = RaxC_GetFirstTimestamp
};

ChunkFuncs *GetChunkClass(CHUNK_TYPES_T chunkType) {
  switch (chunkType) {
    case CHUNK_RAX:         return &raxChunk;
    case CHUNK_REGULAR:     return &regChunk;
    case CHUNK_COMPRESSED:  return &comprChunk;
  }
  return NULL;
}
