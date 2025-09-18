package me.bechberger.jfr.duckdb;

public class RuntimeSQLException extends RuntimeException {
    public RuntimeSQLException(String message, Throwable cause) {
        super(message, cause);
    }
}