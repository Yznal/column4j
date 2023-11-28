package org.column4j.aggregator.vector_api;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.column4j.aggregator.IntAggregator;
import org.column4j.mutable.primitive.IntMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class VectorMinAggregator implements IntAggregator {

    public final VectorSpecies<Integer> speciesPreferred;
    public final int speciesLength;

    public VectorMinAggregator() {
        this.speciesPreferred = IntVector.SPECIES_PREFERRED;
        this.speciesLength = speciesPreferred.length();
    }

    @Override
    public int aggregate(IntMutableColumn column, int from, int to) {
        var data = column.getData();
        var tombstone = column.getTombstone();
        from = Math.max(from, column.firstRowIndex());
        var intVector = IntVector.broadcast(speciesPreferred, Integer.MAX_VALUE);
        for(; from < to && from + speciesLength <= to; from += speciesLength) {
            var nextIntVector = IntVector.fromArray(speciesPreferred, data, from);
            var tombstoneMask = nextIntVector.eq(tombstone).not();
            intVector = intVector.lanewise(VectorOperators.MIN, nextIntVector, tombstoneMask);
        }
        var min = intVector.reduceLanes(VectorOperators.MIN);
        // tail
        for(; from < to; from++) {
            var value = data[from];
            min = value == tombstone ? min : Math.min(min, value);
        }
        return min;
    }
}
