/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.util;


import com.intel.hadoop.graphbuilder.pipeline.GraphConstructionPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class RuntimeConfig {
    private static final Logger LOG = Logger.getLogger(RuntimeConfig.class);
    private static RuntimeConfig uniqueInstanceOfRuntimeConfig = null;
    private HashMap<String, String> config;
    private Configuration hadoopConf;
    private Properties properties;

    public static synchronized RuntimeConfig getInstance() {
        createInstance(null);
        return uniqueInstanceOfRuntimeConfig;
    }

    public static synchronized RuntimeConfig getInstance(Class klass) {
        createInstance(klass);
        return uniqueInstanceOfRuntimeConfig;
    }

    public static synchronized RuntimeConfig getInstanceWithDefaultConfig(HashMap<String, String> defaultConfigs) {
        getInstance();
        uniqueInstanceOfRuntimeConfig.addConfigMap(defaultConfigs);
        return uniqueInstanceOfRuntimeConfig;
    }


    public static synchronized void createInstance(Class klass){
        if (uniqueInstanceOfRuntimeConfig == null) {
            if(klass != null){
                uniqueInstanceOfRuntimeConfig = new RuntimeConfig(klass);
            } else{
                uniqueInstanceOfRuntimeConfig = new RuntimeConfig();
            }
        } else {
            if(klass != null){
                uniqueInstanceOfRuntimeConfig.reflectSetConfigHash(klass);
            }
        }
    }

    private void initProperties() {
        if (properties == null) {
            properties = new Properties();
        }
    }

    private void initConfig() {
        if (config == null) {
            config = new HashMap<String, String>();
            hadoopConf = new Configuration();
        }
    }

    private RuntimeConfig() {
        initProperties();
        initConfig();
    }

    private RuntimeConfig(Class klass) {
        initProperties();
        initConfig();
        reflectSetConfigHash(klass);
    }

    private String splitKey(String key) {
        String[] nameSpacedKey = key.split("\\.");
        if (nameSpacedKey.length > 0) {
            String combinedKey = nameSpacedKey[1].toUpperCase();
            for (int i = 2; i < nameSpacedKey.length; i++) {
                combinedKey += "_" + nameSpacedKey[i].toUpperCase();
            }
            return combinedKey;
        } else {
            return null;
        }
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }

    public GraphConstructionPipeline addConfig(GraphConstructionPipeline conf){
        for(Map.Entry<String, String> option : hadoopConf.getValByRegex("^graphbuilder.").entrySet()){
            String key = option.getKey();
            String value = option.getValue();
            conf.addUserOpt(key, value);
        }
        return conf;
    }

    public void loadConfig(Reducer.Context context){
        loadConfig(context.getConfiguration());
    }
    public void loadConfig(Mapper.Context context) {
        loadConfig(context.getConfiguration());
    }

    public void loadConfig(Configuration conf) {
        if( conf != null){
            for (Map.Entry<String, String> option : conf.getValByRegex("^graphbuilder.").entrySet()) {
                String key = option.getKey();
                String value = option.getValue();
                hadoopConf.set(key, value);
                config.put(splitKey(key), value);
            }
        }
    }

    public void addConfigMap(HashMap<String, String> configMap) {
        Iterator it = configMap.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            if (pair.getValue() != null && !config.containsKey(pair.getKey().toString())) {
                config.put(pair.getKey().toString(), pair.getValue().toString());
            }
        }
    }

    public Integer getPropertyInt(String property) {
        return Integer.parseInt(properties.getProperty(property, config.get(property)));
    }

    public String getPropertyString(String property) {
        return properties.getProperty(property, config.get(property));
    }

    public String getProperty(String property) {
        return getPropertyString(property);
    }

    public HashMap<String, String> getAllConfigUnderNamespace(String prefix) {
        HashMap<String, String> filteredConfigMap = new HashMap<>();
        Iterator it = config.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry configPair = (Map.Entry) it.next();
            String storedKey = configPair.getKey().toString();
            // If storedKey starts with prefix, it implies config belongs to the namespace
            if (storedKey.indexOf(prefix) == 0) {
                String configKey = storedKey.substring(prefix.length()).toLowerCase().replace('_', '.');
                String configValue = getProperty(storedKey);
                filteredConfigMap.put(configKey,configValue);
            }
        }
        return filteredConfigMap;
    }

    private void reflectSetConfigHash(Class klass) {
        for (Field field : klass.getDeclaredFields()) {
            field.setAccessible(true);
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                Object value;
                try {
                    value = field.get(field.getType());
                } catch (IllegalAccessException e) {
                    value = null;
                }
                if (value != null && !config.containsKey(field.getName())) {
                    config.put(field.getName(), value.toString());
                }
            }
        }
    }
}
