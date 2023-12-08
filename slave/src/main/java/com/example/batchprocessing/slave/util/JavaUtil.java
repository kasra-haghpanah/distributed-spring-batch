package com.example.batchprocessing.slave.util;

public class JavaUtil {

    public static Integer getInteger(String string, Integer defaultValue) {
        Integer value = defaultValue;
        try {
            value = Integer.valueOf(string);
        } catch (Exception e) {
        }
        return value;
    }
}
