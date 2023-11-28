package org.column4j.aggregator.vector_api;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.column4j.ColumnVector;
import org.column4j.aggregator.IntAggregator;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class VectorMaxAggregator implements IntAggregator {

    public final VectorSpecies<Integer> speciesPreferred;
    public final int speciesLength;

    public VectorMaxAggregator() {
        this.speciesPreferred = IntVector.SPECIES_PREFERRED;
        this.speciesLength = speciesPreferred.length();
    }

    @Override
    public int aggregate(ColumnVector<int[]> column, int from, int to) {
        var data = column.getData();
        from = Math.max(from, column.firstRowIndex());
        var intVector = IntVector.broadcast(speciesPreferred, Integer.MIN_VALUE);
        for(; from < to && from + speciesLength <= to; from += speciesLength) {
            var nextIntVector = IntVector.fromArray(speciesPreferred, data, from);
            intVector = intVector.max(nextIntVector);
        }
        var max = intVector.reduceLanes(VectorOperators.MAX);
        // tail
        for(; from < to; from++) {
            max = Math.max(max, data[from]);
        }
        return max;
    }
}
