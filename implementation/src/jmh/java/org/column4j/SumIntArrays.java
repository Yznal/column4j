package org.column4j;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.*;

import java.util.Arrays;

/**
 * Benchmark example
 *
 * @author sibmaks
 * @since 0.0.1
 */
@Fork(1)
@State(Scope.Thread)
public class SumIntArrays {
    @Param({ "16", "128", "1024" })
    public int arraySize;

    public int[] array;
    public VectorSpecies<Integer> speciesPreferred;
    public int speciesLength;

    @Setup(Level.Iteration)
    public void setUp() {
        array = new int[arraySize];
        Arrays.fill(array, 1);
        speciesPreferred = IntVector.SPECIES_PREFERRED;
        speciesLength = speciesPreferred.length();
    }

    /**
     * Sum values in array via sequential for-each loop
     *
     * @return sum-result
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int benchmarkSumViaFor() {
        int result = 0;
        for(int i = 0; i < arraySize; i++) {
            result += array[i];
        }
        return result;
    }

    /**
     * Sum values in array with using {@link IntVector} API
     *
     * @return sum-result
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int benchmarkSumViaVector() {
        IntVector aVector = IntVector.fromArray(speciesPreferred, array, 0);
        for(int offset = speciesLength; offset < arraySize; offset += speciesLength) {
            IntVector bVector = IntVector.fromArray(speciesPreferred, array, offset);
            aVector = aVector.add(bVector);
        }
        return aVector.reduceLanes(VectorOperators.ADD);
    }

}
