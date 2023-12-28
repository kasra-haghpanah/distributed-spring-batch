package com.example.batchprocessing.slave.util;

public class JavaUtil {
    public static Integer getInteger(String string, Integer defaultValue) {
        try {
            defaultValue = Integer.valueOf(string);
        } catch (Exception e) {
        }
        return defaultValue;
    }

    public static Long getLong(String string, Long defaultValue) {
        try {
            defaultValue = Long.valueOf(string);
        } catch (Exception e) {
        }
        return defaultValue;
    }

}
