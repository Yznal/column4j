package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Float64MutableColumnImpl;
import org.column4j.column.mutable.primitive.Float64MutableColumn;
import org.column4j.utils.Float64VectorUtils;

public class Float64Aggregator {
    static public int indexOfAnother(Float64MutableColumn column, double value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Float64VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Float64VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            offset += chunkSize;
            res = Float64VectorUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + Float64VectorUtils.indexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
    }

    static public int lastIndexOfAnother(Float64MutableColumn column, double value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Float64VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res =  Float64VectorUtils.lastIndexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }

        for (int i = (to - 1) / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Float64VectorUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + Float64VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }

    static public int indexOf(Float64MutableColumn column, double value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Float64VectorUtils.indexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Float64VectorUtils.indexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            offset += chunkSize;
            res = Float64VectorUtils.indexOf(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + Float64VectorUtils.indexOf(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
    }

    static public int lastIndexOf(Float64MutableColumn column, double value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Float64VectorUtils.lastIndexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res =  Float64VectorUtils.lastIndexOf(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }

        for (int i = (to - 1) / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Float64VectorUtils.lastIndexOf(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + Float64VectorUtils.lastIndexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }

    static public double min(Float64MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final double tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Float64VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize);
        }

        double res = Float64VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = Math.min(res, column.getChunk(i).getStatistic().getMin());
        }

        final double minInTail = Float64VectorUtils.min(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = Math.min(res, minInTail);

        return res;
    }

    static public double max(Float64MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final double tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Float64VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        double res = Float64VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = Math.max(res, column.getChunk(i).getStatistic().getMax());
        }

        final double maxInTail = Float64VectorUtils.max(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = Math.max(res, maxInTail);

        return res;
    }

    static public Float64MutableColumn mul(Float64MutableColumn column1, Float64MutableColumn column2, int elements, int resChunkSize) {
        double[] data1 = column1.getByIndexes(0, elements);
        double[] data2 = column2.getByIndexes(0, elements);
        double[] resData = Float64VectorUtils.mul(data1, data2, 0, 0, elements);
        var resColumn = new Float64MutableColumnImpl(resChunkSize, column1.getTombstone() * column2.getTombstone());
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Float64MutableColumn sum(Float64MutableColumn column1, Float64MutableColumn column2, int elements, int resChunkSize) {
        double[] data1 = column1.getByIndexes(0, elements);
        double[] data2 = column2.getByIndexes(0, elements);
        double[] resData = Float64VectorUtils.sum(data1, data2, 0, 0, elements);
        var resColumn = new Float64MutableColumnImpl(resChunkSize, column1.getTombstone() + column2.getTombstone());
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Float64MutableColumn mul(Float64MutableColumn[] columns, int elements, int resChunkSize) {
        double resTombstone = columns[0].getTombstone();
        double[] resData = columns[0].getByIndexes(0, elements - 1);

        for (int i = 1; i < columns.length; i++) {
            resTombstone *= columns[i].getTombstone();
            double[] data = columns[i].getByIndexes(0, elements);
            resData = Float64VectorUtils.mul(data, resData, 0, 0, elements);
        }

        var resColumn = new Float64MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Float64MutableColumn sum(Float64MutableColumn[] columns, int elements, int resChunkSize) {
        double resTombstone = columns[0].getTombstone();
        double[] resData = columns[0].getByIndexes(0, elements);

        for (int i = 1; i < columns.length; i++) {
            resTombstone += columns[i].getTombstone();
            double[] data = columns[i].getByIndexes(0, elements);
            resData = Float64VectorUtils.sum(data, resData, 0, 0, elements);
        }

        var resColumn = new Float64MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }
}
