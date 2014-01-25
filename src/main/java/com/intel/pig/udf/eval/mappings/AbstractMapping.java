/*
 * Copyright 2014 YarcData LLC All Rights Reserved.
 */

package com.intel.pig.udf.eval.mappings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.intel.hadoop.graphbuilder.types.DoubleType;
import com.intel.hadoop.graphbuilder.types.FloatType;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * Abstract class that provides helper methods for decoding mappings
 * 
 */
public abstract class AbstractMapping {

    /**
     * Map key for properties
     */
    protected static final String PROPERTIES = "properties";

    /**
     * Converts the mapping back into a map that Pig can serialize
     * 
     * @return Map
     */
    public abstract Map<String, Object> toMap();

    /**
     * Extracts the string value for a given key from the given map
     * 
     * @param map
     *            Map
     * @param key
     *            Key
     * @param requireNotNull
     *            Whether a non-null value is required
     * @return String value if available
     * @throws NullPointerException
     *             Thrown if a non-null value is required but a null value is
     *             found
     * @throws IllegalArgumentException
     *             Thrown if the value is not a string
     */
    protected String getStringValue(Map<String, Object> map, String key, boolean requireNotNull) {
        Object value = map.get(key);
        if (value == null) {
            if (requireNotNull)
                throw new NullPointerException("Expected a non-null value for the key " + key);
            return null;
        }
        if (!(value instanceof String))
            throw new IllegalArgumentException("Expected a String value for the key " + key + " but got a "
                    + value.getClass().getCanonicalName());
        return (String) value;
    }

    /**
     * Extracts the boolean value for a given key from the given map
     * 
     * @param map
     *            Map
     * @param key
     *            Key
     * @param defaultValue
     *            Default value to use if the key gives a null value
     * @return Boolean value
     * @throws IllegalArgumentException
     *             Thrown if the key has a non-boolean value
     */
    protected boolean getBooleanValue(Map<String, Object> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        if (value == null)
            return defaultValue;
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            // Pig doesn't support booleans as a native data type so need to
            // support parsing from strings
            return Boolean.parseBoolean((String) value);
        } else {
            throw new IllegalArgumentException("Expected a Boolean value for the key " + key + " but got a "
                    + value.getClass().getCanonicalName());
        }
    }

    /**
     * Extracts a list value for a given key from the given map
     * 
     * @param map
     *            Map
     * @param key
     *            Key
     * @param requireNotNull
     *            Whether a non-null value is required
     * @return List value if available
     * @throws ExecException
     * @throws NullPointerException
     *             Thrown if a non-null value is expected but a null value is
     *             found
     * @throws IllegalArgumentException
     *             Thrown if the value of the key is not of the correct type
     */
    @SuppressWarnings("unchecked")
    protected <T> List<T> getListValue(Map<String, Object> map, String key, boolean requireNotNull) throws ExecException {
        Object value = map.get(key);
        if (value == null) {
            if (requireNotNull)
                throw new NullPointerException("Expected a non-null value for the key " + key);
            return (List<T>) null;
        }
        if (value instanceof Tuple) {
            List<T> values = new ArrayList<T>();
            Tuple tuple = (Tuple) value;
            for (int i = 0; i < tuple.size(); i++) {
                value = tuple.get(i);
                if (value == null)
                    continue;
                values.add((T) value);
            }
            return values;
        } else {
            throw new IllegalArgumentException("Expected a Tuple value for the key " + key + " but got a "
                    + value.getClass().getCanonicalName());
        }
    }

    /**
     * Extracts a map value for a given key from the given map
     * 
     * @param map
     *            Map
     * @param key
     *            Key
     * @param requireNotNull
     *            Whether a non-null value is required
     * @return Map value if available
     * @throws NullPointerException
     *             Thrown if a non-null value is expected but a null value is
     *             found
     * @throws IllegalArgumentException
     *             Thrown if the value of the key is not of the correct type
     */
    @SuppressWarnings("unchecked")
    protected Map<String, Object> getMapValue(Map<String, Object> map, String key, boolean requireNotNull) {
        Object value = map.get(key);
        if (value == null) {
            if (requireNotNull)
                throw new NullPointerException("Expected a non-null value for the key " + key);
            return (Map<String, Object>) null;
        }
        if (value instanceof Map<?, ?>) {
            return (Map<String, Object>) value;
        } else {
            throw new IllegalArgumentException("Expected a Map value for the key " + key + " but got a "
                    + value.getClass().getCanonicalName());
        }
    }

    /**
     * Extracts a map value for a given key from the given map
     * 
     * @param map
     *            Map
     * @param key
     *            Key
     * @param requireNotNull
     *            Whether a non-null value is required
     * @return Map value if available
     * @throws NullPointerException
     *             Thrown if a non-null value is expected but a null value is
     *             found
     * @throws IllegalArgumentException
     *             Thrown if the value of the key is not of the correct type
     */
    @SuppressWarnings("unchecked")
    protected <T> Map<String, T> getTypedMapValue(Map<String, Object> map, String key, boolean requireNotNull) {
        Object value = map.get(key);
        if (value == null) {
            if (requireNotNull)
                throw new NullPointerException("Expected a non-null value for the key " + key);
            return (Map<String, T>) null;
        }
        if (value instanceof Map<?, ?>) {
            Map<String, T> values = new HashMap<String, T>();
            for (Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                values.put(e.getKey(), (T) e.getValue());
            }
            return values;
        } else {
            throw new IllegalArgumentException("Expected a Map value for the key " + key + " but got a "
                    + value.getClass().getCanonicalName());
        }
    }

    protected String tupleToMapKeyValueString(Collection<?> items, String key) {
        return tupleToMapKeyValueString(items.iterator(), key);
    }

    protected String tupleToMapKeyValueString(Iterator<?> items, String key) {
        StringBuilder builder = new StringBuilder();
        builder.append('\'');
        builder.append(key);
        builder.append("' # (");
        while (items.hasNext()) {
            builder.append('\'');
            builder.append(items.next());
            builder.append('\'');
            if (items.hasNext())
                builder.append(',');
            builder.append(' ');
        }
        builder.append(')');

        return builder.toString();
    }

    protected String mapToMapKeyValueString(Map<String, String> map, String key) {
        StringBuilder builder = new StringBuilder();
        builder.append('\'');
        builder.append(key);
        builder.append("' # [ ");

        Iterator<Entry<String, String>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, String> e = iter.next();
            builder.append('\'');
            builder.append(e.getKey());
            builder.append("' # '");
            builder.append(e.getValue());
            builder.append('\'');
            if (iter.hasNext())
                builder.append(',');
            builder.append(' ');
        }

        builder.append(']');
        return builder.toString();
    }

    protected String getStringValue(Tuple tuple, Integer field) throws ExecException {
        if (tuple == null || field == null)
            return null;

        Object value = tuple.get(field);
        if (value == null)
            return null;
        return value.toString();
    }

    @SuppressWarnings("rawtypes")
    protected WritableComparable pigTypesToSerializedJavaTypes(Object value, byte typeByte) throws IllegalArgumentException {
        WritableComparable object = null;

        switch (typeByte) {
        case DataType.BYTE:
            object = new IntType((int) value);
            break;
        case DataType.INTEGER:
            object = new IntType((int) value);
            break;
        case DataType.LONG:
            object = new LongType((long) value);
            break;
        case DataType.FLOAT:
            object = new FloatType((float) value);
            break;
        case DataType.DOUBLE:
            object = new DoubleType((double) value);
            break;
        case DataType.CHARARRAY:
            object = new StringType((String) value);
            break;
        default:
            throw new IllegalArgumentException("Invalid argument exception");

        }

        return object;
    }
}