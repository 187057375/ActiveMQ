package com.sdu.activemq.core;

import com.google.common.base.Strings;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class MQConfig {

    private Properties props = new Properties();

    public MQConfig(String config) throws IOException {
        InputStream is = MQConfig.class.getClassLoader().getResourceAsStream(config);
        props.load(is);
    }

    public MQConfig(Properties props) {
        this.props = props;
    }

    public int getInt(String key, int defaultValue) {
        String value = props.getProperty(key);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = props.getProperty(key);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    public long getLong(String key, long defaultValue) {
        String value = props.getProperty(key);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    public float getFloat(String key, float defaultValue) {
        String value = props.getProperty(key);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }
        return Float.parseFloat(value);
    }

    public double getDouble(String key, double defaultValue) {
        String value = props.getProperty(key);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }
        return Double.parseDouble(value);
    }

    public String getString(String key, String defaultValue) {
        String value = props.getProperty(key);
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }
        return value;
    }

}
