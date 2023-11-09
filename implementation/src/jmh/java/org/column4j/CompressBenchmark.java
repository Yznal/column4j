package org.column4j;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import org.column4j.compression.IntCompressor;
import org.openjdk.jmh.annotations.*;

import java.util.Arrays;

@Fork(1)
@State(Scope.Thread)
public class CompressBenchmark {

    @Param({"10", "16", "1000", "1024", "1000000", "1048576"})
    public int arraySize;

    @Param({"1", "2", "3"})
    public int byteSize;

    public int[] arr;

    public byte[] bytes;

    public IntCompressor compressor;


    @Setup(Level.Iteration)
    public void setUp() {
        arr = new int[arraySize];
        Arrays.fill(arr, (1 << byteSize * 8) - 35);
        compressor = new IntCompressor(byteSize);
        bytes = compressor.compressInts(arr);
    }

    /**
     * Sequentially compresses values by individual ints
     *
     * @return compressed bytes
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public byte[] benchmarkCompressSequential() {
        return compressor.compressInts(arr);
    }

    /**
     * Extracts integer bytes with {@link IntVector} to {@link ByteVector}
     * compression conversion
     *
     * @return compressed bytes
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public byte[] benchmarkCompressVector_maskCompression() {
        return compressor.compressIntsV(arr);
    }

    /**
     * Turns compressed integer bytes into integer array
     * using sequential for loop
     *
     * @return decompressed ints
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int[] benchmarkDecompressSequential() {
        return compressor.decompressInts(bytes);
    }

    /**
     * Turns compressed integer bytes into integer array
     * using vectorized simd loop
     *
     * @return decompressed ints
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int[] benchmarkDecompressVector() {
        return compressor.decompressIntsV(bytes);
    }


}

