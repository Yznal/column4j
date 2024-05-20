package org.column4jBench.array;

import org.openjdk.jmh.annotations.*;

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
public class ReadWriteBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    int[] array1;
    int[] array2;

    @Setup(Level.Invocation)
    public void setUp() {
        var rand = new Random();
        array1 = new int[arraySize];
        array2 = new int[arraySize];
        for (int i = 0; i < arraySize; ++i) {
            array1[i] = rand.nextInt();
            array2[i] = rand.nextInt();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void allocateArray(Blackhole blackhole, ReadWriteBenchmark benchmark) {
        blackhole.consume(new int[benchmark.arraySize]);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void writeArrow(ReadWriteBenchmark benchmark) {
        for (int i = 0; i < benchmark.arraySize; i++) {
            benchmark.array2[i] = benchmark.array1[i];
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void readArrow(Blackhole blackhole, ReadWriteBenchmark benchmark) {
        for (int i = 0; i < benchmark.arraySize; ++i) {
            blackhole.consume(benchmark.array1[i]);
        }
    }
}
