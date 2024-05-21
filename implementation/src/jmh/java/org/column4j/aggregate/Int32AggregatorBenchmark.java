package org.column4j.aggregate;

import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.column4j.column.impl.mutable.primitive.Int32MutableColumnImpl;
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
public class Int32AggregatorBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    @Param({"16", "128", "1024"})
    public int columnChunkSize;

    Random rand = new Random();

    Int32MutableColumnImpl column;

    @Setup(Level.Invocation)
    public void setUp() {
        column = GenerateColumn();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void min(Blackhole blackhole, Int32AggregatorBenchmark benchmark) {
        blackhole.consume(
                Int32Aggregator.min(benchmark.column, 0, benchmark.arraySize)
        );
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void indexOf(Blackhole blackhole, Int32AggregatorBenchmark benchmark) {
        blackhole.consume(
            Int32Aggregator.indexOfAnother(
                    benchmark.column,
                    benchmark.column.get(benchmark.arraySize / 2),
                    0,
                    benchmark.arraySize)
        );
    }

    private Int32MutableColumnImpl GenerateColumn() {
        var column = new Int32MutableColumnImpl(columnChunkSize, Integer.MAX_VALUE);
        for (int i = 0; i < arraySize; ++i) {
            column.write(i, rand.nextInt());
        }
        return column;
    }
}
