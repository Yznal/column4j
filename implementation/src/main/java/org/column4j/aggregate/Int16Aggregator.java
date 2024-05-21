package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Int16MutableColumnImpl;
import org.column4j.column.mutable.primitive.Int16MutableColumn;
import org.column4j.utils.Int16VectorUtils;

public class Int16Aggregator {
    static public int indexOfAnother(Int16MutableColumn column, short value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int16VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Int16VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            offset += chunkSize;
            res = Int16VectorUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        res = Int16VectorUtils.indexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }
        return -1;
    }

    static public int lastIndexOfAnother(Int16MutableColumn column, short value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int16VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res =  Int16VectorUtils.lastIndexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }

        for (int i = (to - 1) / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int16VectorUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        res = Int16VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }
        return -1;
    }

    static public int indexOf(Int16MutableColumn column, short value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int16VectorUtils.indexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Int16VectorUtils.indexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            offset += chunkSize;
            res = Int16VectorUtils.indexOf(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        res = Int16VectorUtils.indexOf(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }
        return -1;
    }

    static public int lastIndexOf(Int16MutableColumn column, short value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int16VectorUtils.lastIndexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res =  Int16VectorUtils.lastIndexOf(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }

        for (int i = (to - 1) / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int16VectorUtils.lastIndexOf(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        res = Int16VectorUtils.lastIndexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }
        return -1;
    }

    static public short min(Int16MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final short tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Int16VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize);
        }

        short res = Int16VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = (short)Math.min(res, column.getChunk(i).getStatistic().getMin());
        }

        final short minInTail = Int16VectorUtils.min(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = (short)Math.min(res, minInTail);

        return res;
    }

    static public short max(Int16MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final short tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Int16VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        short res = Int16VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = (short)Math.max(res, column.getChunk(i).getStatistic().getMax());
        }

        final short maxInTail = Int16VectorUtils.max(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = (short)Math.max(res, maxInTail);

        return res;
    }

    static public Int16MutableColumn mul(Int16MutableColumn column1, Int16MutableColumn column2, int elements, int resChunkSize) {
        short[] data1 = column1.getByIndexes(0, elements);
        short[] data2 = column2.getByIndexes(0, elements);
        short[] resData = Int16VectorUtils.mul(data1, data2, 0, 0, elements);
        var resColumn = new Int16MutableColumnImpl(resChunkSize, (short) (column1.getTombstone() * column2.getTombstone()));
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int16MutableColumn sum(Int16MutableColumn column1, Int16MutableColumn column2, int elements, int resChunkSize) {
        short[] data1 = column1.getByIndexes(0, elements);
        short[] data2 = column2.getByIndexes(0, elements);
        short[] resData = Int16VectorUtils.sum(data1, data2, 0, 0, elements);
        var resColumn = new Int16MutableColumnImpl(resChunkSize, (short) (column1.getTombstone() + column2.getTombstone()));
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int16MutableColumn mul(Int16MutableColumn[] columns, int elements, int resChunkSize) {
        short resTombstone = columns[0].getTombstone();
        short[] resData = columns[0].getByIndexes(0, elements - 1);

        for (int i = 1; i < columns.length; i++) {
            resTombstone *= columns[i].getTombstone();
            short[] data = columns[i].getByIndexes(0, elements);
            resData = Int16VectorUtils.mul(data, resData, 0, 0, elements);
        }

        var resColumn = new Int16MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int16MutableColumn sum(Int16MutableColumn[] columns, int elements, int resChunkSize) {
        short resTombstone = columns[0].getTombstone();
        short[] resData = columns[0].getByIndexes(0, elements);

        for (int i = 1; i < columns.length; i++) {
            resTombstone += columns[i].getTombstone();
            short[] data = columns[i].getByIndexes(0, elements);
            resData = Int16VectorUtils.sum(data, resData, 0, 0, elements);
        }

        var resColumn = new Int16MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }
}
