package org.column4j.utils;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Random;

/**
 * Benchmark for writing/reading data.
 * Benchmarking column4j and apache arrows to compare
 *
 * @author iv4n-t3a
 */
@Fork(1)
@State(Scope.Thread)
public class Int64VectorUtilsBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    @Param({"16", "128", "1024"})
    public int columnChunkSize;

    public long[] array;

    @Setup(Level.Iteration)
    public void setUp() {
        var rand = new Random();
        array = new long[arraySize];
        for (int i = 0; i < arraySize; ++i) {
            array[i] = rand.nextInt(Integer.MAX_VALUE);
        }
    }

    @BenchmarkMode(Mode.Throughput)
    public void benchmarkAgregationColumn4j() {
        Int64VectorUtils.min(array, Long.MAX_VALUE, 0, arraySize);
        Int64VectorUtils.max(array, Long.MAX_VALUE, 0, arraySize);
        Int64VectorUtils.indexOfAnother(array, array[arraySize/2], 0, arraySize);
        Int64VectorUtils.lastIndexOfAnother(array, array[arraySize/2], 0, arraySize);
    }
}
