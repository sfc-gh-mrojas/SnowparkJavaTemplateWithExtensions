package com.snowflake.snowpark_java.extensions;


@FunctionalInterface
public interface MapFunction<T, U> {
    public abstract U call(T value) throws Exception;
}

