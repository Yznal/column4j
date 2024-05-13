package org.column4j.aggregate;

import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Random;

import org.column4j.column.impl.mutable.primitive.Int64MutableColumnImpl;

/**
 * Benchmark for writing/reading data.
 * Benchmarking column4j and apache arrows to compare
 *
 * @author iv4n-t3a
 */
@Fork(1)
@State(Scope.Thread)
public class Int64AggregatorBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    @Param({"16", "128", "1024"})
    public int columnChunkSize;

    Random rand = new Random();

    final int defaultValue = 10;
    Int64MutableColumnImpl column;
    Int64MutableColumnImpl rarecolumn;

    @Setup(Level.Iteration)
    public void setUp() {
        column = GenerateColumn();
        rarecolumn = GenerateRareColumn();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void min() {
        Int64Aggregator.min(column, 0, arraySize);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void indexOfAnother() {
        Int64Aggregator.indexOfAnother(column, defaultValue, 0, arraySize);
    }

    private Int64MutableColumnImpl GenerateColumn() {
        var column = new Int64MutableColumnImpl(columnChunkSize, Long.MAX_VALUE);
        for (int i = 0; i < arraySize; ++i) {
            column.write(i, rand.nextInt());
        }
        return column;
    }

    private Int64MutableColumnImpl GenerateRareColumn() {
        var column = new Int64MutableColumnImpl(columnChunkSize, Long.MAX_VALUE);
        for (int i = 0; i < arraySize; ++i) {
            column.write(defaultValue, i);
        }
        column.write(arraySize / 2, 1);
        return column;
    }
}
