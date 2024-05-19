package org.column4jBench;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators.IntComparator;

import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.algorithm.search.ParallelSearcher;

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

    RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);

    final IntComparator intcomparator = new IntComparator();

    IntVector vector;
    IntVector keyvector;

    @Setup(Level.Invocation)
    public void setUp() {
        if (vector != null) {
            vector.close();
        }
        vector = GenerateVector();
        keyvector = CreateKeyVector();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void linearSearch(Blackhole blackhole, AggregatorBenchmark benchmark) {
        blackhole.consume(
            VectorSearcher.linearSearch(benchmark.vector, benchmark.intcomparator, benchmark.keyvector, 0)
        );
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void liniarSearchFieldsInitialization(Blackhole blackhole, AggregatorBenchmark benchmark) {
        blackhole.consume(
            VectorSearcher.linearSearch(benchmark.vector, new IntComparator(), benchmark.CreateKeyVector(), 0)
        );
    }

    private IntVector GenerateVector() {
        var vector = new IntVector("int vector", alloc);
        vector.allocateNew(arraySize);
        for (int i = 0; i < arraySize; ++i) {
            vector.set(i, rand.nextInt());
        }
        vector.setValueCount(arraySize);
        return vector;
    }

    private IntVector CreateKeyVector() {
        var vector = new IntVector("int vector", alloc);
        vector.allocateNew(1);
        vector.set(1, this.vector.get(arraySize / 2));
        vector.setValueCount(1);
        return vector;
    }
}
