package org.column4j.utils;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for writing/reading data.
 * Benchmarking column4j and apache arrows to compare
 *
 * @author iv4n-t3a
 */
@Fork(value = 3)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class Int32VectorUtilsBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    @Param({"16", "128", "1024"})
    public int columnChunkSize;

    public int[] array;

    @Setup(Level.Invocation)
    public void setUp() {
        var rand = new Random();
        array = new int[arraySize];
        for (int i = 0; i < arraySize; ++i) {
            array[i] = rand.nextInt();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void minByVectorUtils(Blackhole blackhole, Int32VectorUtilsBenchmark benchmark) {
        blackhole.consume(
            Int32VectorUtils.min(benchmark.array, Integer.MAX_VALUE, 0, benchmark.arraySize)
        );
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void indexOfByVectorUtils(Blackhole blackhole, Int32VectorUtilsBenchmark benchmark) {
        blackhole.consume(
            Int32VectorUtils.indexOf(benchmark.array, benchmark.array[benchmark.arraySize / 2], 0, benchmark.arraySize)
        );
    }
}
