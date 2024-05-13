package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Int32MutableColumnImpl;
import org.column4j.column.mutable.primitive.Int32MutableColumn;
import org.column4j.utils.Int32VectorUtils;

public class Int32Aggregator {
    static public int indexOfAnother(Int32MutableColumn column, int value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int32VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Int32VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            offset += chunkSize;
            res = Int32VectorUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + Int32VectorUtils.indexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
    }

    static public int lastIndexOfAnother(Int32MutableColumn column, int value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int32VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res =  Int32VectorUtils.lastIndexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }

        for (int i = (to - 1) / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int32VectorUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + Int32VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }

    static public int min(Int32MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final int tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Int32VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize);
        }

        int res = Int32VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = Math.min(res, column.getChunk(i).getStatistic().getMin());
        }

        final int minInTail = Int32VectorUtils.min(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = Math.min(res, minInTail);

        return res;
    }

    static public int max(Int32MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final int tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Int32VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Int32VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = Math.max(res, column.getChunk(i).getStatistic().getMax());
        }

        final int maxInTail = Int32VectorUtils.max(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = Math.max(res, maxInTail);

        return res;
    }

    static public Int32MutableColumn mul(Int32MutableColumn column1, Int32MutableColumn column2, int elements, int resChunkSize) {
        int[] data1 = column1.getByIndexes(0, elements);
        int[] data2 = column2.getByIndexes(0, elements);
        int[] resData = Int32VectorUtils.mul(data1, data2, 0, 0, elements);
        var resColumn = new Int32MutableColumnImpl(resChunkSize, column1.getTombstone() * column2.getTombstone());
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int32MutableColumn sum(Int32MutableColumn column1, Int32MutableColumn column2, int elements, int resChunkSize) {
        int[] data1 = column1.getByIndexes(0, elements);
        int[] data2 = column2.getByIndexes(0, elements);
        int[] resData = Int32VectorUtils.sum(data1, data2, 0, 0, elements);
        var resColumn = new Int32MutableColumnImpl(resChunkSize, column1.getTombstone() + column2.getTombstone());
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int32MutableColumn mul(Int32MutableColumn[] columns, int elements, int resChunkSize) {
        int resTombstone = columns[0].getTombstone();
        int[] resData = columns[0].getByIndexes(0, elements - 1);

        for (int i = 1; i < columns.length; i++) {
            resTombstone *= columns[i].getTombstone();
            int[] data = columns[i].getByIndexes(0, elements);
            resData = Int32VectorUtils.mul(data, resData, 0, 0, elements);
        }

        var resColumn = new Int32MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int32MutableColumn sum(Int32MutableColumn[] columns, int elements, int resChunkSize) {
        int resTombstone = columns[0].getTombstone();
        int[] resData = columns[0].getByIndexes(0, elements);

        for (int i = 1; i < columns.length; i++) {
            resTombstone += columns[i].getTombstone();
            int[] data = columns[i].getByIndexes(0, elements);
            resData = Int32VectorUtils.sum(data, resData, 0, 0, elements);
        }

        var resColumn = new Int32MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }
}
