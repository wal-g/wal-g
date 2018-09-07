#include "./encode.h"
#include "./encode_go.h"

using namespace brotli;

int CBrotliCompressBuffer(CBrotliParams params,
                         size_t input_size,
                         const uint8_t* input_buffer,
                         size_t* encoded_size,
                         uint8_t* encoded_buffer) {
  return BrotliCompressBuffer(
    *((BrotliParams*) &params),
    input_size,
    input_buffer,
    encoded_size,
    encoded_buffer
  );
}

int CBrotliCompressBufferDict(CBrotliParams params,
                         size_t input_size,
                         const uint8_t* input_buffer,
                         size_t dict_size,
                         const uint8_t* dict_buffer,
                         size_t* encoded_size,
                         uint8_t* encoded_buffer) {
 if (*encoded_size == 0) {
     // Output buffer needs at least one byte.
     return 0;
   }
   BrotliMemIn in(input_buffer, input_size);
   BrotliMemOut out(encoded_buffer, *encoded_size);
   if (!BrotliCompressWithCustomDictionary(dict_size, dict_buffer, *((BrotliParams*) &params), &in, &out)) {
     return 0;
   }
   *encoded_size = out.position();
   return 1;
}

CBrotliCompressor CBrotliCompressorNew(CBrotliParams params) {
  BrotliCompressor *ret = new BrotliCompressor(*((BrotliParams*) &params));
  return (CBrotliCompressor) ret;
}

void CBrotliCompressorFree(CBrotliCompressor cbp) {
  BrotliCompressor *bp = (BrotliCompressor *)cbp;
  delete bp;
}

size_t CBrotliCompressorGetInputBlockSize(CBrotliCompressor cbp) {
  BrotliCompressor *bp = (BrotliCompressor *)cbp;
  return bp->input_block_size();
}

void CBrotliCompressorCopyInputToRingBuffer(CBrotliCompressor cbp, const size_t input_size, const uint8_t* input_buffer) {
  BrotliCompressor *bp = (BrotliCompressor *)cbp;
  bp->CopyInputToRingBuffer(input_size, input_buffer);
}

bool CBrotliCompressorWriteBrotliData(CBrotliCompressor cbp, const bool is_last, const bool force_flush, size_t* out_size, uint8_t** output) {
  BrotliCompressor *bp = (BrotliCompressor *)cbp;
  return bp->WriteBrotliData(is_last, force_flush, out_size, output);
}
