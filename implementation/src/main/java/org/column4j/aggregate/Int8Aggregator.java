package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Int8MutableColumnImpl;
import org.column4j.column.mutable.primitive.Int8MutableColumn;
import org.column4j.utils.Int8VectorUtils;

public class Int8Aggregator {
    static public int indexOfAnother(Int8MutableColumn column, byte value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int8VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Int8VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            offset += chunkSize;
            res = Int8VectorUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + Int8VectorUtils.indexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
    }

    static public int lastIndexOfAnother(Int8MutableColumn column, byte value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int8VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res =  Int8VectorUtils.lastIndexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }

        for (int i = (to - 1) / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int8VectorUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + Int8VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }

    static public int indexOf(Int8MutableColumn column, byte value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int8VectorUtils.indexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Int8VectorUtils.indexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            offset += chunkSize;
            res = Int8VectorUtils.indexOf(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + Int8VectorUtils.indexOf(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
    }

    static public int lastIndexOf(Int8MutableColumn column, byte value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int8VectorUtils.lastIndexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res =  Int8VectorUtils.lastIndexOf(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }

        for (int i = (to - 1) / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int8VectorUtils.lastIndexOf(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + Int8VectorUtils.lastIndexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }

    static public byte min(Int8MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final byte tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Int8VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize);
        }

        byte res = Int8VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = (byte)Math.min(res, column.getChunk(i).getStatistic().getMin());
        }

        final byte minInTail = Int8VectorUtils.min(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = (byte)Math.min(res, minInTail);

        return res;
    }

    static public byte max(Int8MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final byte tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Int8VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        byte res = Int8VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = (byte)Math.max(res, column.getChunk(i).getStatistic().getMax());
        }

        final byte maxInTail = Int8VectorUtils.max(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = (byte)Math.max(res, maxInTail);

        return res;
    }

    static public Int8MutableColumn mul(Int8MutableColumn column1, Int8MutableColumn column2, int elements, int resChunkSize) {
        byte[] data1 = column1.getByIndexes(0, elements);
        byte[] data2 = column2.getByIndexes(0, elements);
        byte[] resData = Int8VectorUtils.mul(data1, data2, 0, 0, elements);
        var resColumn = new Int8MutableColumnImpl(resChunkSize, (byte) (column1.getTombstone() * column2.getTombstone()));
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int8MutableColumn sum(Int8MutableColumn column1, Int8MutableColumn column2, int elements, int resChunkSize) {
        byte[] data1 = column1.getByIndexes(0, elements);
        byte[] data2 = column2.getByIndexes(0, elements);
        byte[] resData = Int8VectorUtils.sum(data1, data2, 0, 0, elements);
        var resColumn = new Int8MutableColumnImpl(resChunkSize, (byte) (column1.getTombstone() + column2.getTombstone()));
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int8MutableColumn mul(Int8MutableColumn[] columns, int elements, int resChunkSize) {
        byte resTombstone = columns[0].getTombstone();
        byte[] resData = columns[0].getByIndexes(0, elements - 1);

        for (int i = 1; i < columns.length; i++) {
            resTombstone *= columns[i].getTombstone();
            byte[] data = columns[i].getByIndexes(0, elements);
            resData = Int8VectorUtils.mul(data, resData, 0, 0, elements);
        }

        var resColumn = new Int8MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int8MutableColumn sum(Int8MutableColumn[] columns, int elements, int resChunkSize) {
        byte resTombstone = columns[0].getTombstone();
        byte[] resData = columns[0].getByIndexes(0, elements);

        for (int i = 1; i < columns.length; i++) {
            resTombstone += columns[i].getTombstone();
            byte[] data = columns[i].getByIndexes(0, elements);
            resData = Int8VectorUtils.sum(data, resData, 0, 0, elements);
        }

        var resColumn = new Int8MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }
}
