package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Static methods for evaluating date/time functions.
 * Package-private methods for use within the evaluator package.
 */
public class DateTimeFunctions {
    /**
     * Gets the current timestamp
     */
    static CellValue evaluateNow(List<CellValue> arguments) {
        if (arguments.size() != 0) {
            throw new IllegalArgumentException("NOW function requires no arguments");
        }
        return new CellValue.TimestampValue(Instant.now());
    }

    /**
     * Extracts the year from a timestamp
     */
    static CellValue evaluateYear(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("YEAR function requires exactly 1 argument");
        }
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getYear());
    }

    /**
     * Extracts the month from a timestamp
     */
    static CellValue evaluateMonth(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("MONTH function requires exactly 1 argument");
        }
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getMonthValue());
    }

    /**
     * Extracts the day from a timestamp
     */
    static CellValue evaluateDay(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("DAY function requires exactly 1 argument");
        }
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfMonth());
    }

    /**
     * Extracts the hour from a timestamp
     */
    static CellValue evaluateHour(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("HOUR function requires exactly 1 argument");
        }
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getHour());
    }

    /**
     * Extracts the minute from a timestamp
     */
    static CellValue evaluateMinute(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("MINUTE function requires exactly 1 argument");
        }
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getMinute());
    }

    /**
     * Extracts the second from a timestamp
     */
    static CellValue evaluateSecond(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("SECOND function requires exactly 1 argument");
        }
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getSecond());
    }

    /**
     * Calculates the difference between two timestamps
     */
    static CellValue evaluateDateDiff(List<CellValue> arguments) {
        if (arguments.size() != 3) {
            throw new IllegalArgumentException("DATE_DIFF function requires exactly 3 arguments");
        }
        if (!(arguments.get(0) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("DATE_DIFF first argument must be a string (unit)");
        }
        String unit = ((CellValue.StringValue) arguments.get(0)).value().toUpperCase();
        Instant start = FunctionUtils.toInstant(arguments.get(1));
        Instant end = FunctionUtils.toInstant(arguments.get(2));
        return new CellValue.NumberValue(switch (unit) {
            case "SECONDS" -> ChronoUnit.SECONDS.between(start, end);
            case "MINUTES" -> ChronoUnit.MINUTES.between(start, end);
            case "HOURS" -> ChronoUnit.HOURS.between(start, end);
            case "DAYS" -> ChronoUnit.DAYS.between(start, end);
            default -> throw new IllegalArgumentException("Invalid unit: " + unit);
        });
    }

    /**
     * Adds a duration to a timestamp
     */
    static CellValue evaluateDateAdd(List<CellValue> arguments) {
        if (arguments.size() != 3) {
            throw new IllegalArgumentException("DATE_ADD function requires exactly 3 arguments");
        }
        Instant date = FunctionUtils.toInstant(arguments.get(0));
        if (!(arguments.get(1) instanceof CellValue.NumberValue)) {
            throw new IllegalArgumentException("DATE_ADD second argument must be a number (amount)");
        }
        int amount = (int) ((CellValue.NumberValue) arguments.get(1)).value();
        if (!(arguments.get(2) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("DATE_ADD third argument must be a string (unit)");
        }
        String unit = ((CellValue.StringValue) arguments.get(2)).value().toUpperCase();
        Instant result = switch (unit) {
            case "SECONDS" -> date.plus(amount, ChronoUnit.SECONDS);
            case "MINUTES" -> date.plus(amount, ChronoUnit.MINUTES);
            case "HOURS" -> date.plus(amount, ChronoUnit.HOURS);
            case "DAYS" -> date.plus(amount, ChronoUnit.DAYS);
            default -> throw new IllegalArgumentException("Invalid unit: " + unit);
        };
        return new CellValue.TimestampValue(result);
    }
}
