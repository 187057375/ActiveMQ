package com.sdu.activemq.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author hanhan.zhang
 * */
public class GsonUtils {

    private static final Gson GSON = new GsonBuilder().create();

    private static final Gson PRETTY = new GsonBuilder().setPrettyPrinting().create();


    public static String toJson(Object obj) {
        return GSON.toJson(obj);
    }

    public static String toPrettyJson(Object obj) {
        return PRETTY.toJson(obj);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        return GSON.fromJson(json, clazz);
    }

}
