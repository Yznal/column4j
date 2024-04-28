package org.column4j.aggregate;

import org.column4j.column.mutable.primitive.Float32MutableColumn;
import org.column4j.utils.Float32VectorUtils;

public class Float32Aggregator {
    static public int indexOfAnother(Float32MutableColumn column, float value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + Float32VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res = Float32VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            offset += chunkSize;
            res = Float32VectorUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + Float32VectorUtils.indexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
    }

    static public int lastIndexOfAnother(Float32MutableColumn column, float value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + Float32VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res =  Float32VectorUtils.lastIndexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
        if (res != -1) {
            return offset + res;
        }

        for (int i = to / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Float32VectorUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + Float32VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }

    static public float min(Float32MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final float tombstone = column.getTombstone();

        if (to / chunkSize == from / chunkSize) {
            return Float32VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, to % chunkSize);
        }

        float res = Float32VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            res = Math.min(res, column.getChunk(i).getStatistic().getMin());
        }

        final float minInTail = Float32VectorUtils.min(column.getChunk(to / chunkSize).getData(), tombstone, 0, to % chunkSize);
        res = Math.min(res, minInTail);

        return res;
    }

    static public float max(Float32MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final float tombstone = column.getTombstone();

        if (to / chunkSize == from / chunkSize) {
            return Float32VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, to % chunkSize);
        }

        float res = Float32VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            res = Math.max(res, column.getChunk(i).getStatistic().getMax());
        }

        final float maxInTail = Float32VectorUtils.max(column.getChunk(to / chunkSize).getData(), tombstone, 0, to % chunkSize);
        res = Math.max(res, maxInTail);

        return res;
    }
}