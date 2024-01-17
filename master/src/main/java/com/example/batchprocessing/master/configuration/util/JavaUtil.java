package com.example.batchprocessing.master.configuration.util;

import java.util.Collection;

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

    public static boolean isEmpty(Collection collection){
        if(collection == null || collection.size() ==0){
            return true;
        }
        return false;
    }
}
