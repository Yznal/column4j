package org.column4j;


import org.column4j.index.v3.chunk.primitive.impl.skip.SkipInt32ChunkIndex;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.stream.IntStream;

@Fork(1)
@State(Scope.Thread)
public class SkipChunkIndex {

    private static final Random generator = new Random(134);

    @Param({"1000", "1024", "1000000", "1048576", "65000000", "67108864"})
    public int arraySize;

    @Param({"1024", "1048576"})
    public int segmentSize;

    public int existingElement;
    public int absentElement;

    public int[] array;

    SkipInt32ChunkIndex skipIndex;

    /*
    Generate values in [0:1_000_000) and [2_000_000:10_000_000)
    Take absent value as random from [1_000_000:2_000_000)
     */
    @Setup(Level.Iteration)
    public void setUp() {
        array = IntStream.range(0, arraySize)
                .map(i -> {
                    int num1 = generator.nextInt(0, 1_000_000);
                    int num2 = generator.nextInt(2_000_000, 10_000_000);
                    int num = generator.nextDouble() < 0.6 ? num1 : num2; // skew a little
                    return num;
                })
                .toArray();
        existingElement = array[generator.nextInt(arraySize)];
        absentElement = generator.nextInt(1_000_000, 2_000_000);
        skipIndex = new SkipInt32ChunkIndex(array, segmentSize);

    }


    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public boolean skipIndexContainsExisting() {
        return skipIndex.contains(existingElement);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public boolean skipIndexContainsAbsent() {
        return skipIndex.contains(absentElement);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int[] skipIndexGetExisting() {
        return skipIndex.lookupValues(existingElement);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int[] skipIndexGetAbsent() {
        return skipIndex.lookupValues(absentElement);
    }






}

