package ru.itmo.column4j.impl;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import ru.itmo.column4j.ColumnType;

import java.util.function.Function;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class ColumnTypes<T> implements ColumnType<T> {
    public static final ColumnType<Integer> INT = new ColumnTypes<>(Integer.class, ColumnTypes::toInt);
    public static final ColumnType<Long> LONG = new ColumnTypes<>(Long.class, ColumnTypes::toLong);
    public static final ColumnType<Double> DOUBLE = new ColumnTypes<>(Double.class, ColumnTypes::toDouble);
    public static final ColumnType<Float> FLOAT = new ColumnTypes<>(Float.class, ColumnTypes::toFloat);
    public static final ColumnType<Boolean> BOOLEAN = new ColumnTypes<>(Boolean.class, ColumnTypes::toBoolean);
    public static final ColumnType<String> STRING = new ColumnTypes<>(String.class, ColumnTypes::toString);

    private final Class<T> type;
    private final Function<Object, T> caster;

    @Override
    public T cast(Object value) {
        return caster.apply(value);
    }

    private static Integer toInt(Object value) {
        if(value == null) {
            return null;
        }
        if(value instanceof Number number) {
            return number.intValue();
        }
        if(value instanceof CharSequence charSequence) {
            return Integer.parseInt(charSequence.toString());
        }
        throw new IllegalArgumentException("Value '%s' can't be casted to Integer");
    }

    private static Long toLong(Object value) {
        if(value == null) {
            return null;
        }
        if(value instanceof Number number) {
            return number.longValue();
        }
        if(value instanceof CharSequence charSequence) {
            return Long.parseLong(charSequence.toString());
        }
        throw new IllegalArgumentException("Value '%s' can't be casted to Long");
    }

    private static Double toDouble(Object value) {
        if(value == null) {
            return null;
        }
        if(value instanceof Number number) {
            return number.doubleValue();
        }
        if(value instanceof CharSequence charSequence) {
            return Double.parseDouble(charSequence.toString());
        }
        throw new IllegalArgumentException("Value '%s' can't be casted to Double");
    }

    private static Boolean toBoolean(Object value) {
        if(value == null) {
            return null;
        }
        if(value instanceof Boolean bool) {
            return bool;
        }
        if(value instanceof CharSequence charSequence) {
            return Boolean.parseBoolean(charSequence.toString());
        }
        throw new IllegalArgumentException("Value '%s' can't be casted to Boolean");
    }

    private static String toString(Object value) {
        if(value == null) {
            return null;
        }
        return value.toString();
    }

    private static Float toFloat(Object value) {
        if(value == null) {
            return null;
        }
        if(value instanceof Number number) {
            return number.floatValue();
        }
        if(value instanceof CharSequence charSequence) {
            return Float.parseFloat(charSequence.toString());
        }
        throw new IllegalArgumentException("Value '%s' can't be casted to Float");
    }
}
