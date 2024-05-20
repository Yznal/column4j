package org.column4jBench.arrow;

import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators.IntComparator;

import org.apache.arrow.algorithm.search.VectorSearcher;

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
    IntVector vector_to_write_in;
    IntVector keyvector;

    @Setup(Level.Invocation)
    public void setUp() {
        if (vector != null) {
            vector.close();
        }
        vector = generateVector();
        keyvector = createKeyVector();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void linearSearch(Blackhole blackhole, AggregatorBenchmark benchmark) {
        blackhole.consume(
            VectorSearcher.linearSearch(benchmark.vector, benchmark.intcomparator, benchmark.keyvector, 0)
        );
    }

    private IntVector generateVector() {
        var vector = new IntVector("int vector", alloc);
        vector.allocateNew(arraySize);
        for (int i = 0; i < arraySize; ++i) {
            vector.set(i, rand.nextInt());
        }
        vector.setValueCount(arraySize);
        return vector;
    }

    private IntVector createEmptyVector() {
        var vector = new IntVector("int vector", alloc);
        vector.allocateNew(arraySize);
        vector.setValueCount(arraySize);
        return vector;
    }

    private IntVector createKeyVector() {
        var vector = new IntVector("int vector", alloc);
        vector.allocateNew(1);
        vector.set(1, this.vector.get(arraySize / 2));
        vector.setValueCount(1);
        return vector;
    }
}
