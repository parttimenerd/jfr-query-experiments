package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.engine.exception.FunctionArgumentException;
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
        FunctionUtils.assertArgumentCount("NOW", arguments, 0);
        return new CellValue.TimestampValue(Instant.now());
    }

    /**
     * Extracts the year from a timestamp
     */
    static CellValue evaluateYear(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("YEAR", arguments, 1);
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getYear());
    }

    /**
     * Extracts the month from a timestamp
     */
    static CellValue evaluateMonth(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("MONTH", arguments, 1);
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getMonthValue());
    }

    /**
     * Extracts the day from a timestamp
     */
    static CellValue evaluateDay(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("DAY", arguments, 1);
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfMonth());
    }

    /**
     * Extracts the hour from a timestamp
     */
    static CellValue evaluateHour(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("HOUR", arguments, 1);
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getHour());
    }

    /**
     * Extracts the minute from a timestamp
     */
    static CellValue evaluateMinute(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("MINUTE", arguments, 1);
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getMinute());
    }

    /**
     * Extracts the second from a timestamp
     */
    static CellValue evaluateSecond(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("SECOND", arguments, 1);
        Instant instant = FunctionUtils.toInstant(arguments.get(0));
        return new CellValue.NumberValue(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).getSecond());
    }

    /**
     * Calculates the difference between two timestamps
     */
    static CellValue evaluateDateDiff(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("DATE_DIFF", arguments, 3);
        CellValue.StringValue unitValue = FunctionUtils.getStringArgument("DATE_DIFF", arguments, 0);
        String unit = unitValue.value().toUpperCase();
        Instant start = FunctionUtils.toInstant(arguments.get(1));
        Instant end = FunctionUtils.toInstant(arguments.get(2));
        return new CellValue.NumberValue(switch (unit) {
            case "SECONDS" -> ChronoUnit.SECONDS.between(start, end);
            case "MINUTES" -> ChronoUnit.MINUTES.between(start, end);
            case "HOURS" -> ChronoUnit.HOURS.between(start, end);
            case "DAYS" -> ChronoUnit.DAYS.between(start, end);
            default -> throw FunctionArgumentException.forInvalidValue("DATE_DIFF", 0, unit, "valid time unit (SECONDS, MINUTES, HOURS, DAYS)", null);
        });
    }

    /**
     * Adds a duration to a timestamp
     */
    static CellValue evaluateDateAdd(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("DATE_ADD", arguments, 3);
        Instant date = FunctionUtils.toInstant(arguments.get(0));
        CellValue.NumberValue amountValue = FunctionUtils.getNumberArgument("DATE_ADD", arguments, 1);
        int amount = (int) amountValue.value();
        CellValue.StringValue unitValue = FunctionUtils.getStringArgument("DATE_ADD", arguments, 2);
        String unit = unitValue.value().toUpperCase();
        Instant result = switch (unit) {
            case "SECONDS" -> date.plus(amount, ChronoUnit.SECONDS);
            case "MINUTES" -> date.plus(amount, ChronoUnit.MINUTES);
            case "HOURS" -> date.plus(amount, ChronoUnit.HOURS);
            case "DAYS" -> date.plus(amount, ChronoUnit.DAYS);
            default -> throw FunctionArgumentException.forInvalidValue("DATE_ADD", 2, unit, "valid time unit (SECONDS, MINUTES, HOURS, DAYS)", null);
        };
        return new CellValue.TimestampValue(result);
    }
}
