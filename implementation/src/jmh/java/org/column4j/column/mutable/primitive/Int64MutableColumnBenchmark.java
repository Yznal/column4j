package org.column4j.column.mutable.primitive;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Random;

import org.column4j.column.impl.mutable.primitive.Int64MutableColumnImpl;
import org.column4j.utils.Int64VectorUtils;

/**
 * Benchmark for writing/reading data.
 * Benchmarking column4j and apache arrows to compare
 *
 * @author iv4n-t3a
 */
@Fork(1)
@State(Scope.Thread)
public class Int64MutableColumnBenchmark {
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

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void writeColumn4j() {
        createAndFillColumn();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void writeReadColumn4j() {
        var column = createAndFillColumn();
        for (int i = 0; i < arraySize; ++i) {
            long ignore = column.get(i);
        }
    }

    private Int64MutableColumnImpl createAndFillColumn() {
        var column = new Int64MutableColumnImpl(columnChunkSize, Long.MAX_VALUE);
        for (int i = 0; i < arraySize; ++i) {
            column.write(i, array[i]);
        }
        return column;
    }
}
