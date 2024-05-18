package org.column4jBench;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

/**
 * Benchmark for writing/reading data.
 */
@Fork(1)
@State(Scope.Thread)
public class ReadWriteBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    int[] array;
    RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);

    @Setup(Level.Iteration)
    public void setUp() {
        var rand = new Random();
        array = new int[arraySize];
        for (int i = 0; i < arraySize; ++i) {
            array[i] = rand.nextInt(Integer.MAX_VALUE);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void writeArrow() {
        var vector = createAndFillIntVector();
        vector.close();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void writeReadArrow() {
        var vector = createAndFillIntVector();
        for (int i = 0; i < arraySize; ++i) {
            int ignore = vector.get(i);
        }
        vector.close();
    }

    private IntVector createAndFillIntVector() {
        var vector = new IntVector("int vector", alloc);
        vector.allocateNew(arraySize);
        for (int i = 0; i < arraySize; ++i) {
            vector.set(i, array[i]);
        }
        vector.setValueCount(arraySize);
        return vector;
    }
}