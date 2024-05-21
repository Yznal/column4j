package org.column4jBench.arrow;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

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

    int[] array;
    RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    IntVector vector = new IntVector("int vector", alloc);

    @Setup(Level.Invocation)
    public void setUp() {
        if (vector != null) {
            vector.close();
        }
        vector.allocateNew(arraySize);
        var rand = new Random();
        array = new int[arraySize];
        for (int i = 0; i < arraySize; ++i) {
            array[i] = rand.nextInt();
            vector.set(i, rand.nextInt());
        }
        vector.setValueCount(arraySize);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void allocateArrow(Blackhole blackhole, ReadWriteBenchmark benchmark) {
        var vector = new IntVector("int vector", benchmark.alloc);
        vector.allocateNew(benchmark.arraySize);
        blackhole.consume(vector);
        vector.close();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void writeArrow(ReadWriteBenchmark benchmark) {
        for (int i = 0; i < benchmark.arraySize; i++) {
            benchmark.vector.set(i, benchmark.array[i]);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void readArrow(Blackhole blackhole, ReadWriteBenchmark benchmark) {
        for (int i = 0; i < benchmark.arraySize; ++i) {
            blackhole.consume(benchmark.vector.get(i));
        }
    }
}
