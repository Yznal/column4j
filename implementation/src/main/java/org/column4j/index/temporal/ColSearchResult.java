package org.column4j.index.temporal;

/*
Disclaimer: это потенциально просто структурка для удобства восприятия, можно и выписать в int[]
 */
public record ColSearchResult(int colId, int chunksStart, int chunksEnd) {
}
