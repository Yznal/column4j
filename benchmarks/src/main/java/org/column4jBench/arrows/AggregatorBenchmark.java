package org.column4jBench;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators.IntComparator;

import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.algorithm.search.ParallelSearcher;

/**
 * Benchmark for writing/reading data.
 */
@Fork(1)
@State(Scope.Thread)
public class AggregatorBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    Random rand = new Random();

    RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);

    int defaultValue = 10;
    int searchedValue = 11;
    IntVector vector;
    IntVector keyvector;
    IntVector rarevector;

    @Setup(Level.Iteration)
    public void setUp() {
        vector = GenerateVector();
        rarevector = GenerateRareVector();
        keyvector = CreateKeyVector();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void linearSearch() {
        VectorSearcher.linearSearch(rarevector, new IntComparator(), keyvector, 0);
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

    private IntVector GenerateRareVector() {
        var vector = new IntVector("int vector", alloc);
        vector.allocateNew(arraySize);
        for (int i = 0; i < arraySize; ++i) {
            vector.set(i, defaultValue);
        }
        vector.set(arraySize / 2, searchedValue);
        vector.setValueCount(arraySize);
        return vector;
    }

    private IntVector CreateKeyVector() {
        var vector = new IntVector("int vector", alloc);
        vector.allocateNew(1);
        vector.set(1, defaultValue);
        vector.setValueCount(1);
        return vector;
    }
}
