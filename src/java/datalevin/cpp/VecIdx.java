package datalevin.cpp;

import org.bytedeco.javacpp.*;

import java.nio.ByteBuffer;

import datalevin.dtlvnative.DTLV;

public class VecIdx {

    static void expect(boolean mustTrue, String message) {
        if (mustTrue)
            return;

        message = (message != null ? message : "unexpected result");
        throw new RuntimeException("usearch error: " + message);
    }

    static void expectNoError(PointerPointer<BytePointer> error, String message) {

        BytePointer errPtr = error.get(BytePointer.class);
        if (errPtr != null) {
            String msg = errPtr.getString();
            throw new RuntimeException(message + ": " + msg);
        }
    }

    static DTLV.usearch_init_options_t createOpts(final long dimensions,
            final int metricType, final int quantization, final long connectivity,
            final long expansionAdd, final long expansionSearch) {

        DTLV.usearch_init_options_t opts = new DTLV.usearch_init_options_t();

        opts.metric_kind(metricType)
                .metric((DTLV.usearch_metric_t) null)
                .quantization(quantization)
                .dimensions(dimensions)
                .connectivity(connectivity)
                .expansion_add(expansionAdd)
                .expansion_search(expansionSearch)
                .multi(false);

        return opts;
    }

    public static DTLV.usearch_index_t create(final long dimensions,
            final int metricType, final int quantization, final long connectivity,
            final long expansionAdd, final long expansionSearch) {

        DTLV.usearch_init_options_t opts = createOpts(dimensions,
                metricType, quantization, connectivity, expansionAdd, expansionSearch);

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_index_t index = DTLV.usearch_init(opts, error);
        expectNoError(error, "Failed to init index");

        return index;
    }

    public static void free(DTLV.usearch_index_t index) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_free(index, error);
        expectNoError(error, "Fail to free index");
    }

    public static void save(DTLV.usearch_index_t index, String fname) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_save(index, fname, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void load(DTLV.usearch_index_t index, String fname) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_load(index, fname, error);
        expectNoError(error, "Fail to load index");
    }

    public static long serializedLength(DTLV.usearch_index_t index) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        long length = DTLV.usearch_serialized_length(index, error);
        expectNoError(error, "Fail to get serialized length");
        return length;
    }

    public static void saveBuffer(DTLV.usearch_index_t index,
                                  Pointer buffer, long length) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_save_buffer(index, buffer, length, error);
        expectNoError(error, "Fail to save index to buffer");
    }

    public static void loadBuffer(DTLV.usearch_index_t index,
                                  Pointer buffer, long length) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_load_buffer(index, buffer, length, error);
        expectNoError(error, "Fail to load index from buffer");
    }

    public static void addDouble(DTLV.usearch_index_t index, long key, double[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        DoublePointer vecPtr = new DoublePointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f64_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void addFloat(DTLV.usearch_index_t index, long key, float[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        FloatPointer vecPtr = new FloatPointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f32_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void addShort(DTLV.usearch_index_t index, long key, short[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        ShortPointer vecPtr = new ShortPointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_f16_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void addInt8(DTLV.usearch_index_t index, long key, byte[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        BytePointer vecPtr = new BytePointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_i8_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void addByte(DTLV.usearch_index_t index, long key, byte[] vector) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_reserve(index, 1, error);
        expectNoError(error, "Fail to reserve capacity");

        BytePointer vecPtr = new BytePointer(vector);
        error.put(0, (BytePointer) null);
        DTLV.usearch_add(index, key, vecPtr, DTLV.usearch_scalar_b1_k, error);
        expectNoError(error, "Fail to add vector");
    }

    public static void remove(DTLV.usearch_index_t index, long key) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DTLV.usearch_remove(index, key, error);
        expectNoError(error, "Fail to add vector");
    }

    public static double[] getDouble(DTLV.usearch_index_t index,
        long key, int dimensions) {

        double[] result = new double[dimensions];
        DoublePointer resPtr = new DoublePointer(result);
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                      DTLV.usearch_scalar_f64_k, error);
        expectNoError(error, "Fail to get vector");
        if (found == 1) {
            resPtr.get(result);
            return result;
        } else {
            return null;
        }
    }

    public static float[] getFloat(DTLV.usearch_index_t index,
        long key, int dimensions) {

        float[] result = new float[dimensions];
        FloatPointer resPtr = new FloatPointer(result);
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                      DTLV.usearch_scalar_f32_k, error);
        expectNoError(error, "Fail to get vector");
        if (found == 1) {
            resPtr.get(result);
            return result;
        } else {
            return null;
        }
    }

    public static short[] getShort(DTLV.usearch_index_t index,
        long key, int dimensions) {

        short[] result = new short[dimensions];
        ShortPointer resPtr = new ShortPointer(result);
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                      DTLV.usearch_scalar_f16_k, error);
        expectNoError(error, "Fail to get vector");
        if (found == 1) {
            resPtr.get(result);
            return result;
        } else {
            return null;
        }
    }

    public static byte[] getInt8(DTLV.usearch_index_t index,
        long key, int dimensions) {

        byte[] result = new byte[dimensions];
        BytePointer resPtr = new BytePointer(result);
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                      DTLV.usearch_scalar_i8_k, error);
        expectNoError(error, "Fail to get vector");
        if (found == 1) {
            resPtr.get(result);
            return result;
        } else {
            return null;
        }
    }

    public static byte[] getByte(DTLV.usearch_index_t index,
        long key, int dimensions) {

        byte[] result = new byte[dimensions];
        BytePointer resPtr = new BytePointer(result);
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        long found = DTLV.usearch_get(index, key, 1, (Pointer)resPtr,
                                      DTLV.usearch_scalar_b1_k, error);
        expectNoError(error, "Fail to get vector");
        if (found == 1) {
            resPtr.get(result);
            return result;
        } else {
            return null;
        }
    }

    public static SearchResult searchDouble(DTLV.usearch_index_t index,
            double[] query, int count) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        DoublePointer vecPtr = new DoublePointer(query);
        long[] keys = new long[count];
        LongPointer keyPtr = new LongPointer(keys);
        float[] distances = new float[count];
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f64_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        keyPtr.get(keys);
        distPtr.get(distances);
        return new SearchResult(keys, distances);
    }

    public static SearchResult searchFloat(DTLV.usearch_index_t index,
            float[] query, int count) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        FloatPointer vecPtr = new FloatPointer(query);
        long[] keys = new long[count];
        LongPointer keyPtr = new LongPointer(keys);
        float[] distances = new float[count];
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f32_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        keyPtr.get(keys);
        distPtr.get(distances);
        return new SearchResult(keys, distances);
    }

    public static SearchResult searchShort(DTLV.usearch_index_t index,
            short[] query, int count) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        ShortPointer vecPtr = new ShortPointer(query);
        long[] keys = new long[count];
        LongPointer keyPtr = new LongPointer(keys);
        float[] distances = new float[count];
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_f16_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        keyPtr.get(keys);
        distPtr.get(distances);
        return new SearchResult(keys, distances);
    }

    public static SearchResult searchInt8(DTLV.usearch_index_t index,
            byte[] query, int count) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        BytePointer vecPtr = new BytePointer(query);
        long[] keys = new long[count];
        LongPointer keyPtr = new LongPointer(keys);
        float[] distances = new float[count];
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_i8_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        keyPtr.get(keys);
        distPtr.get(distances);
        return new SearchResult(keys, distances);
    }

    public static SearchResult searchByte(DTLV.usearch_index_t index,
            byte[] query, int count) {
        PointerPointer<BytePointer> error = new PointerPointer<>(1);
        error.put(0, (BytePointer) null);
        BytePointer vecPtr = new BytePointer(query);
        long[] keys = new long[count];
        LongPointer keyPtr = new LongPointer(keys);
        float[] distances = new float[count];
        FloatPointer distPtr = new FloatPointer(distances);
        DTLV.usearch_search(index, vecPtr, DTLV.usearch_scalar_b1_k,
                count, keyPtr, distPtr, error);
        expectNoError(error, "Fail to search vector");
        keyPtr.get(keys);
        distPtr.get(distances);
        return new SearchResult(keys, distances);
    }

    public static IndexInfo info(DTLV.usearch_index_t index) {

        PointerPointer<BytePointer> error = new PointerPointer<>(1);

        error.put(0, (BytePointer) null);
        long size = DTLV.usearch_size(index, error);
        expectNoError(error, "Fail to get index size");

        error.put(0, (BytePointer) null);
        long capacity = DTLV.usearch_capacity(index, error);
        expectNoError(error, "Fail to get index capacity");

        error.put(0, (BytePointer) null);
        long memory = DTLV.usearch_memory_usage(index, error);
        expect(memory > 0, "Failed to get index memory usage");

        error.put(0, (BytePointer) null);
        BytePointer hardware = DTLV.usearch_hardware_acceleration(index, error);
        expectNoError(error, "Fail to get hardware ISA name");

        return new IndexInfo(size, capacity, memory, hardware.getString());
    }

    public static class SearchResult {

        private long[] keys;
        private float[] dists;

        public SearchResult(long[] keys, float[] dists) {
            this.keys = keys;
            this.dists = dists;
        }

        public long[] getKeys() {
            return keys;
        }

        public float[] getDists() {
            return dists;
        }
    }

    public static class IndexInfo {

        private long size;
        private long capacity;
        private long memory;
        private String hardware;

        public IndexInfo(long size, long capacity, long memory, String hardware) {
            this.size = size;
            this.capacity = capacity;
            this.memory = memory;
            this.hardware = hardware;
        }

        public long getSize() {
            return size;
        }

        public long getCapacity() {
            return capacity;
        }

        public long getMemory() {
            return memory;
        }

        public String getHardware() {
            return hardware;
        }
    }

}
