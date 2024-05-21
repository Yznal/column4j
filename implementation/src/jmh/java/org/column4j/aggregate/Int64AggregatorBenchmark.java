package org.column4j.aggregate;

import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.column4j.column.impl.mutable.primitive.Int64MutableColumnImpl;
import org.openjdk.jmh.infra.Blackhole;

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
public class Int64AggregatorBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    @Param({"16", "128", "1024"})
    public int columnChunkSize;

    Random rand = new Random();

    Int64MutableColumnImpl column;

    @Setup(Level.Invocation)
    public void setUp() {
        column = GenerateColumn();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void min(Blackhole blackhole, Int64AggregatorBenchmark benchmark) {
        blackhole.consume(
                Int64Aggregator.min(benchmark.column, 0, benchmark.arraySize)
        );
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void indexOf(Blackhole blackhole, Int64AggregatorBenchmark benchmark) {
        blackhole.consume(
                Int64Aggregator.indexOfAnother(
                        benchmark.column,
                        benchmark.column.get(benchmark.arraySize / 2),
                        0,
                        benchmark.arraySize)
        );
    }

    private Int64MutableColumnImpl GenerateColumn() {
        var column = new Int64MutableColumnImpl(columnChunkSize, Long.MAX_VALUE);
        for (int i = 0; i < arraySize; ++i) {
            column.write(i, rand.nextInt());
        }
        return column;
    }
}
