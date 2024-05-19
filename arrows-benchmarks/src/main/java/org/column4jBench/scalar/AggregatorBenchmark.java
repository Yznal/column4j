package org.column4jBench.scalar;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark for writing/reading data.
 */
@Fork(value = 3)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AggregatorBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    Random rand = new Random();
    int[] array;

    @Setup(Level.Invocation)
    public void setUp() {
        array = new int[arraySize];
        for (int i = 0; i < array.length; i++) {
            array[i] = rand.nextInt();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void linearSearch(Blackhole blackhole, AggregatorBenchmark benchmark) {
        int value = benchmark.array[benchmark.arraySize / 2];
        int res = 0;
        for (int i = 0; i < benchmark.arraySize; i++) {
            if (benchmark.array[i] == value) {
                res = i;
                break;
            }
        }
        blackhole.consume(res);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void min(Blackhole blackhole, AggregatorBenchmark benchmark) {
        int res = Integer.MAX_VALUE;
        for (int i = 0; i < benchmark.arraySize; i++) {
            res = Math.min(benchmark.array[i], res);
        }
        blackhole.consume(res);
    }
}
