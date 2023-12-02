package org.column4j.index;

public record Dimension(CharSequence dimensionName, CharSequence dimensionValue) {

    public static Dimension dim(CharSequence dimensionName, CharSequence dimensionValue) {
        return new Dimension(dimensionName, dimensionValue);
    }
}
