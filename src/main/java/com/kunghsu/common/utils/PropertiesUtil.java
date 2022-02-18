package com.kunghsu.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * 读取配置文件
 * author:xuyaokun_kzx
 * date:2021/9/18
 * desc:
*/
public class PropertiesUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);


    //
    private static final String DEFAULT_PROPERTIES_NAME = "application.properties";
    private static Properties properties;

    static {
        try {
            InputStream inputStream = PropertiesUtil.class.getResourceAsStream(DEFAULT_PROPERTIES_NAME);
//            properties = PropertiesLoaderUtils.loadAllProperties(DEFAULT_PROPERTIES_NAME);
            properties = new Properties();
            properties.load(inputStream);
        } catch (Exception e) {
            LOGGER.error("加载配置文件异常", e);
        }
    }

    public static String get(String key, String defaultValue){

        String value = null;
        value = properties.getProperty(key, defaultValue);
        return value;
    }

    /**
     * 加载其他配置文件里的属性
     * @param key
     * @param defaultValue
     * @param propertyPath
     * @return
     */
    public static String get(String key, String defaultValue, String propertyPath){

        String value = null;
        Properties properties = null;
        try {
//            properties = PropertiesLoaderUtils.loadAllProperties(propertyPath);
            value = properties.getProperty(key, defaultValue);
        } catch (Exception e) {
            LOGGER.error("加载配置文件异常", e);
        }
        return value;
    }
}
