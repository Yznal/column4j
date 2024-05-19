package org.column4j.column.mutable.primitive;

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
public class Int32MutableColumnBenchmark {
    @Param({"16", "128", "1024"})
    public int arraySize;

    @Param({"16", "128", "1024"})
    public int columnChunkSize;

    public int[] array;
    Int32MutableColumnImpl column;

    @Setup(Level.Invocation)
    public void setUp() {
        var rand = new Random();
        array = new int[arraySize];
        for (int i = 0; i < arraySize; ++i) {
            array[i] = rand.nextInt();
            column.write(i, rand.nextInt());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void allocColumn4j(Blackhole blackhole, Int32MutableColumnBenchmark benchmark) {
        blackhole.consume(
                new Int32MutableColumnImpl(benchmark.arraySize, benchmark.columnChunkSize)
        );
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void writeColumn4j(Int32MutableColumnBenchmark benchmark) {
        for (int i = 0; i < benchmark.arraySize; i++) {
            benchmark.column.write(i, benchmark.array[i]);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    static public void readColumn4j(Blackhole blackhole, Int32MutableColumnBenchmark benchmark) {
        for (int i = 0; i < benchmark.arraySize; ++i) {
            blackhole.consume(benchmark.column.get(i));
        }
    }
}
