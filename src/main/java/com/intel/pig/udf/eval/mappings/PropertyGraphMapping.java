/*
 * Copyright 2014 YarcData LLC All Rights Reserved.
 */

package com.intel.pig.udf.eval.mappings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * <p>
 * Represents a property graph mapping
 * </p>
 * <h3>Mapping Format</h3>
 * <p>
 * Mappings are expressed as Pig maps, the mapping contains one/both of the
 * {@code vertices} or {@code edges} keys the values of which are tuples
 * containing one/more mappings e.g.
 * </p>
 * 
 * <pre>
 * [ 'vertices' # ( ), 'edges' # ( ) ]
 * </pre>
 * <p>
 * The tuples contain vertex/edge mappings respectively, see the documentation
 * for {@link VertexMapping} and {@link EdgeMapping} to see the expected format
 * of the mappings within those tuples. The use of tuples means that each tuple
 * of input data can generate multiple vertices and/or edges within the property
 * graph.
 * </p>
 */
public class PropertyGraphMapping extends AbstractMapping {

    /**
     * Map key for the vertex mappings tuple
     */
    protected static final String VERTICES = "vertices";
    /**
     * Map key for the edge mappings tuple
     */
    protected static final String EDGES = "edges";

    private List<VertexMapping> vertexMappings = new ArrayList<VertexMapping>();
    private List<EdgeMapping> edgeMappings = new ArrayList<EdgeMapping>();

    /**
     * Creates a property graph mapping
     * 
     * @param vertexMappings
     *            Vertex mappings
     * @param edgeMappings
     *            Edge mappings
     */
    public PropertyGraphMapping(Collection<VertexMapping> vertexMappings, Collection<EdgeMapping> edgeMappings) {
        if (vertexMappings != null)
            this.vertexMappings.addAll(vertexMappings);
        if (edgeMappings != null)
            this.edgeMappings.addAll(edgeMappings);
    }

    /**
     * Creates a new property graph mapping directly from an Object
     * <p>
     * This constructor assumes that the passed object comes from the processing
     * of a Pig script and thus will be a Map generated from Pig. See the
     * documentation for {@link PropertyGraphMapping} for details of the map
     * format expected.
     * <p>
     * 
     * @param object
     * @throws ExecException
     */
    @SuppressWarnings("unchecked")
    public PropertyGraphMapping(Object object) throws ExecException {
        if (object == null)
            throw new NullPointerException("Cannot create a property graph mapping from a null object");
        if (!(object instanceof Map<?, ?>))
            throw new IllegalArgumentException("Cannot create a property graph mapping from a non-map object of type " + object.getClass().getCanonicalName());

        Map<String, Object> mapping = (Map<String, Object>) object;
        List<Object> vs = this.getListValue(mapping, VERTICES, false);
        if (vs != null) {
            for (Object v : vs) {
                this.vertexMappings.add(new VertexMapping(v));
            }
        }
        List<Object> es = this.getListValue(mapping, EDGES, false);
        if (es != null) {
            for (Object e : es) {
                this.edgeMappings.add(new EdgeMapping(e));
            }
        }
    }

    /**
     * Gets the vertex mappings
     * 
     * @return Iterator of vertex mappings
     */
    public Iterator<VertexMapping> getVertexMappings() {
        return this.vertexMappings.iterator();
    }

    /**
     * Gets the edge mappings
     * 
     * @return Iterator of edge mappings
     */
    public Iterator<EdgeMapping> getEdgeMappings() {
        return this.edgeMappings.iterator();
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

        Iterator<VertexMapping> vs = this.getVertexMappings();
        while (vs.hasNext()) {
            vs.next().apply(input, fieldMapping, output);
        }
        Iterator<EdgeMapping> es = this.getEdgeMappings();
        while (es.hasNext()) {
            es.next().apply(input, fieldMapping, output);
        }
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        if (this.vertexMappings.size() > 0) {
            builder.append('\'');
            builder.append(VERTICES);
            builder.append("' # (");
            Iterator<VertexMapping> vs = this.vertexMappings.iterator();
            while (vs.hasNext()) {
                builder.append(vs.next().toString());
                if (vs.hasNext())
                    builder.append(", ");
            }
            builder.append(" )");
            if (this.edgeMappings.size() > 0)
                builder.append(", ");
        }
        if (this.edgeMappings.size() > 0) {
            builder.append('\'');
            builder.append(EDGES);
            builder.append("' # (");
            Iterator<EdgeMapping> es = this.edgeMappings.iterator();
            while (es.hasNext()) {
                builder.append(es.next().toString());
                if (es.hasNext())
                    builder.append(", ");
            }
            builder.append(" )");
        }
        builder.append(" ]");
        return builder.toString();
    }
}
