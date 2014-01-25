/*
 * Copyright 2014 YarcData LLC All Rights Reserved.
 */

package com.intel.pig.udf.eval.mappings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * <p>
 * Represents a vertex mapping used in generating property graphs
 * <p>
 * <h3>Mapping Format</h3>
 * <p>
 * Mappings are expressed as Pig maps, the mapping contains at least a
 * {@code id} key and potentially the optional {@code properties} and
 * {@code labels} keys e.g.
 * </p>
 * 
 * <pre>
 * [ 'id' # 'ssn', 'properties' # ( 'name', 'age', 'dob' ), 'labels' # [ 'type' # 'Person' ] ]
 * </pre>
 * 
 * <h4>id</h4>
 * <p>
 * The {@code id} key provides a field name that will be used to extract a
 * vertex ID for generating a vertex. This key is required.
 * </p>
 * <h4>properties</h4>
 * <p>
 * The {@code properties} key provides a tuple consisting of field names which
 * should be used to add properties to the generated vertex. Field names are
 * used as the resulting property names with property values taking from the
 * actual input data being mapped.
 * </p>
 * <h4>labels</h4>
 * <p>
 * The {@code labels} key provides a map containing key value pairs which are
 * used to add constant properties to the generated vertex e.g. adding a type
 * descriptor as shown in the above example. Each key is used as the property
 * name and the corresponding value as the value for the property.
 * </p>
 */
public class VertexMapping extends AbstractMapping {

    /**
     * Map key for the id field
     */
    protected static final String ID_FIELD = "id";
    /**
     * Map key for labels
     */
    protected static final String LABELS = "labels";

    private String idField;
    private List<String> properties = new ArrayList<String>();
    private Map<String, String> labels = new HashMap<String, String>();

    /**
     * Creates a new vertex mapping
     * 
     * @param idField
     *            ID Field
     * @param properties
     *            Properties
     * @param labels
     *            Labels
     */
    public VertexMapping(String idField, Collection<String> properties, Map<String, String> labels) {
        if (idField == null)
            throw new NullPointerException("ID Field cannot be null for a vertex mapping");
        this.idField = idField;
        if (properties != null)
            this.properties.addAll(properties);
        if (labels != null)
            this.labels.putAll(labels);
    }

    /**
     * Creates a new vertex mapping
     * 
     * @param idField
     *            ID Field
     */
    public VertexMapping(String idField) {
        this(idField, null, null);
    }

    /**
     * Creates a new vertex mapping directly from an Object
     * <p>
     * This constructor assumes that the passed object comes from the processing
     * of a Pig script and thus will be a Map generated from Pig. See the
     * documentation for {@link VertexMapping} for details of the map format
     * expected.
     * <p>
     * 
     * @param object
     * @throws ExecException
     */
    @SuppressWarnings("unchecked")
    public VertexMapping(Object object) throws ExecException {
        if (object == null)
            throw new NullPointerException("Cannot create a vertex mapping from a null object");
        if (!(object instanceof Map<?, ?>))
            throw new IllegalArgumentException("Cannot create a vertex mapping from a non-map object");

        Map<String, Object> vertexMapping = (Map<String, Object>) object;
        this.idField = this.getStringValue(vertexMapping, ID_FIELD, true);
        List<String> ps = this.getListValue(vertexMapping, PROPERTIES, false);
        if (ps != null)
            this.properties.addAll(ps);
        Map<String, String> ls = this.getTypedMapValue(vertexMapping, LABELS, false);
        if (ls != null)
            this.labels.putAll(ls);
    }

    /**
     * Gets the ID field for the mapping
     * <p>
     * This is the field from the data that will be used to assign the ID to the
     * generated vertex
     * </p>
     * 
     * @return ID field
     */
    public String getIdField() {
        return this.idField;
    }

    /**
     * Gets the properties defined for this mapping
     * <p>
     * These are the field names that will be used to add data driven properties
     * to the vertex i.e. these are properties whose values are populated from
     * fields in the source data.
     * </p>
     * 
     * @return Iterator of properties
     */
    public Iterator<String> getProperties() {
        return this.properties.iterator();
    }

    /**
     * Gets the labels defined for this mapping
     * <p>
     * Labels are fixed value properties that will be added to generated
     * vertices. These can be used to assign properties to chunks of the
     * generated graph, for example adding some form of type identifier property
     * to vertices.
     * </p>
     * 
     * @return Labels (unmodifiable copy)
     */
    public Map<String, String> getLabels() {
        return Collections.unmodifiableMap(this.labels);
    }

    /**
     * Applies the mapping to the given input
     * 
     * @param input
     *            Input
     * @param fieldMapping
     *            Field Mapping
     * @param output
     *            Output
     * @throws ExecException
     */
    public void apply(Tuple input, Map<String, Integer> fieldMapping, DataBag output) throws ExecException {
        if (input == null)
            return;

        String idValue = this.getStringValue(input, fieldMapping.get(this.idField));
        if (idValue == null)
            return;

        Vertex<StringType> vertex = new Vertex<StringType>(new StringType(idValue));
        Iterator<String> ps = this.getProperties();
        while (ps.hasNext()) {
            String pName = ps.next();
            Integer pIndex = fieldMapping.get(pName);
            if (pIndex == null)
                continue;
            Object pValue = input.get(pIndex);
            if (pValue == null)
                continue;
            vertex.setProperty(pName, this.pigTypesToSerializedJavaTypes(pValue, input.getType(pIndex)));
        }

        Map<String, String> labels = this.getLabels();
        for (Entry<String, String> e : labels.entrySet()) {
            vertex.setProperty(e.getKey(), new StringType(e.getValue()));
        }

        // Output the generated vertex
        SerializedGraphElementStringTypeVids element = new SerializedGraphElementStringTypeVids();
        element.init(vertex);
        output.add(TupleFactory.getInstance().newTuple(element));
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> mapping = new HashMap<String, Object>();
        mapping.put(ID_FIELD, this.idField);
        if (this.properties.size() > 0)
            mapping.put(PROPERTIES, TupleFactory.getInstance().newTuple(this.properties));
        if (this.labels.size() > 0)
            mapping.put(LABELS, TupleFactory.getInstance().newTuple(this.labels));
        return mapping;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        builder.append(" '");
        builder.append(ID_FIELD);
        builder.append(" ' # '");
        builder.append(this.idField);
        builder.append("'");
        if (this.properties.size() > 0) {
            builder.append(", ");
            builder.append(this.tupleToMapKeyValueString(this.properties, PROPERTIES));
        }
        if (this.labels.size() > 0) {
            builder.append(", ");
            builder.append(this.mapToMapKeyValueString(this.labels, LABELS));
        }
        builder.append(" ]");
        return builder.toString();
    }
}
