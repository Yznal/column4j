package org.column4j.index;

import org.column4j.index.v1.SimpleInvertedIndex;
import org.column4j.index.v2.BlockInvertedIndex;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.column4j.index.Dimension.dim;
import static org.junit.jupiter.api.Assertions.*;

class InvertedIndexTest {

    private static final String HOST = "host";
    private static final String DC = "dc";
    private static final String METRIC = "metric";

    private static final Map<List<Dimension>, Integer> entries = Map.of(
            List.of(dim(HOST, "ameba1"), dim(METRIC, "cpu_use")), 1,
            List.of(dim(HOST, "ameba2"), dim(METRIC, "cpu_use")), 2,
            List.of(dim(HOST, "ameba1"), dim(METRIC, "memory_use")), 3,
            List.of(dim(HOST, "ameba2"), dim(METRIC, "memory_use")), 4,
            List.of(dim(HOST, "ameba3"), dim(METRIC, "memory_use")), 5,
            List.of(dim(DC, "A"), dim(HOST, "ameba1"), dim(METRIC, "req_count")), 6,
            List.of(dim(DC, "B"), dim(HOST, "ameba2"), dim(METRIC, "req_count")), 7
    );

    private static Stream<InvertedIndex> testIndices() {
        return Stream.of(
                new SimpleInvertedIndex(),
                new BlockInvertedIndex()
        ).peek(index -> entries.forEach(index::insertValue));
    }


    @MethodSource("testIndices")
    @ParameterizedTest
    void testSimpleLookup(InvertedIndex index) {
        entries.forEach(
                (query, id) -> {
                    int[] res = index.lookup(query);
                    assertEquals(1, res.length);
                    assertEquals(id, res[0]);
                }
        );
    }

    @MethodSource("testIndices")
    @ParameterizedTest
    void testAggLookup(InvertedIndex index) {
        var query1 = List.of(dim(HOST, "ameba1"));
        int[] expected1 = {1, 3, 6};
        int[] actual1 = index.lookup(query1);
        assertEquals(3, actual1.length);
        assertArrayEquals(expected1, actual1);

        var query2 = List.of(dim(DC, "A"));
        int[] actual2 = index.lookup(query2);
        assertEquals(1, actual2.length);
        assertEquals(6, actual2[0]);
    }

    @MethodSource("testIndices")
    @ParameterizedTest
    void testEmptyLookup(InvertedIndex index) {
        var query = List.of(dim("country", "au"));
        int[] res = index.lookup(query);
        assertEquals(0, res.length);

        var partialQuery = List.of(
                dim(HOST, "ameba2"), dim(METRIC, "cpu_use"), dim("country", "au")
        );
        int[] partialRes = index.lookup(partialQuery);
        assertEquals(0, partialRes.length);

        var nonIntersectedQuery = List.of(dim(HOST, "ameba3"), dim(METRIC, "cpu_use"));
        int[] nonIntersectedRes = index.lookup(nonIntersectedQuery);
        assertEquals(0, nonIntersectedRes.length);

        int[] emptyRes = index.lookup(Collections.emptyList());
        assertEquals(0, emptyRes.length);
    }
}