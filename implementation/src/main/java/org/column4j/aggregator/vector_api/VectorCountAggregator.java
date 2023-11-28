package org.column4j.aggregator.vector_api;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;
import org.column4j.aggregator.IntAggregator;
import org.column4j.mutable.primitive.IntMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class VectorCountAggregator implements IntAggregator {

    public final VectorSpecies<Integer> speciesPreferred;
    public final int speciesLength;

    public VectorCountAggregator() {
        this.speciesPreferred = IntVector.SPECIES_PREFERRED;
        this.speciesLength = speciesPreferred.length();
    }

    @Override
    public int aggregate(IntMutableColumn column, int from, int to) {
        var data = column.getData();
        var tombstone = column.getTombstone();
        from = Math.max(from, column.firstRowIndex());
        var count = 0;
        for(; from < to && from + speciesLength <= to; from += speciesLength) {
            var intVector = IntVector.fromArray(speciesPreferred, data, from);
            count += intVector.eq(tombstone)
                    .not()
                    .trueCount();
        }
        // tail
        for(; from < to; from++) {
            count += data[from] == tombstone ? 0 : 1;
        }
        return count;
    }
}
