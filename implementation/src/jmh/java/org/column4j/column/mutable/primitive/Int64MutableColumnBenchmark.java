package org.column4j.column.mutable.primitive;

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
public class Int64MutableColumnBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    @Param({"16", "128", "1024"})
    public int columnChunkSize;

    final int maxChunkSize = 23;
    final long thombstone = 1234567;

    public long[] array;
    Int64MutableColumnImpl column = new Int64MutableColumnImpl(maxChunkSize, thombstone);

    @Setup(Level.Invocation)
    public void setUp() {
        var rand = new Random();
        array = new long[arraySize];
        for (int i = 0; i < arraySize; ++i) {
            array[i] = rand.nextLong();
            column.write(i, rand.nextLong());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void allocColumn4j(Blackhole blackhole, Int64MutableColumnBenchmark benchmark) {
        blackhole.consume(
                new Int64MutableColumnImpl(benchmark.arraySize, benchmark.columnChunkSize)
        );
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void writeColumn4j(Int64MutableColumnBenchmark benchmark) {
        for (int i = 0; i < benchmark.arraySize; i++) {
            benchmark.column.write(i, benchmark.array[i]);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void readColumn4j(Blackhole blackhole, Int64MutableColumnBenchmark benchmark) {
        for (int i = 0; i < benchmark.arraySize; ++i) {
            blackhole.consume(benchmark.column.get(i));
        }
    }
}
