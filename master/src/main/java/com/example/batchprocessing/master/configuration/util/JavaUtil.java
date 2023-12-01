package com.example.batchprocessing.master.configuration.util;

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
