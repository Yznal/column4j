package org.column4j;

import jdk.incubator.vector.IntVector;
import org.column4j.compression.IntCompressor;
import org.openjdk.jmh.annotations.*;

import java.util.Arrays;

@Fork(1)
@State(Scope.Thread)
public class Compress {

    @Param({"10", "16", "100", "128", "1000", "1024", "1000000", "1048576"})
    public int arraySize;

    public int[] arr;

    public IntCompressor compressor = new IntCompressor(2);


    @Setup(Level.Iteration)
    public void setUp() {
        arr = new int[arraySize];
        Arrays.fill(arr, 31231);
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
     * Sum values in array with using {@link IntVector} API
     *
     * @return compressed bytes
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public byte[] benchmarkCompressVector() {
        return compressor.compressIntsV(arr);
    }

}

