package org.column4j.aggregate;

import org.column4j.column.mutable.StringMutableColumn;
import org.column4j.utils.StringUtils;

public class StringAggregator {
    static public int indexOfAnother(StringMutableColumn column, String value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + StringUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res = StringUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            offset += chunkSize;
            res = StringUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + StringUtils.indexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
    }

    static public int lastIndexOfAnother(StringMutableColumn column, String value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + StringUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res =  StringUtils.lastIndexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
        if (res != -1) {
            return offset + res;
        }

        for (int i = to / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = StringUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + StringUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }
}
