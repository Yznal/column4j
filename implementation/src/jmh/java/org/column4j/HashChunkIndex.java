package org.column4j;


import org.column4j.index.v3.chunk.primitive.impl.hash.HashInt32ChunkIndex;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.stream.IntStream;

@Fork(1)
@State(Scope.Thread)
public class HashChunkIndex {

    private static final Random generator = new Random(134);

    @Param({"1000", "1024", "1000000", "1048576"})
    public int arraySize;

    @Param({"1024", "1048576"})
    public int segmentSize;

    public int existingElement;
    public int absentElement;

    public int[] array;

    HashInt32ChunkIndex hashIndex;

    /*
    Generate values in [0:1_000_000) and [2_000_000:10_000_000)
    Take absent value as random from [1_000_000:2_000_000)
     */
    @Setup(Level.Iteration)
    public void setUp() {
        hashIndex = new HashInt32ChunkIndex();
        array = IntStream.range(0, arraySize)
                .map(i -> {
                    int num1 = generator.nextInt(0, 1_000_000);
                    int num2 = generator.nextInt(2_000_000, 10_000_000);
                    int num = generator.nextDouble() < 0.6 ? num1 : num2; // skew a little
                    hashIndex.insertRecord(i, num);
                    return num;
                })
                .toArray();
        existingElement = array[generator.nextInt(arraySize)];
        absentElement = generator.nextInt(1_000_000, 2_000_000);
    }


    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public boolean hashIndexContainsExisting() {
        return hashIndex.contains(existingElement);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public boolean hashIndexContainsAbsent() {
        return hashIndex.contains(absentElement);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int[] hashIndexGetExisting() {
        return hashIndex.lookupValues(existingElement);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int[] hashIndexGetAbsent() {
        return hashIndex.lookupValues(absentElement);
    }



}
