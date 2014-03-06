/*
 * Copyright 2014 YarcData LLC 
 * All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */

package com.intel.pig.udf.eval.mappings;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.jena.iri.IRI;
import org.apache.jena.iri.IRIFactory;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import com.hp.hpl.jena.sparql.util.NodeFactoryExtra;
import com.hp.hpl.jena.vocabulary.RDF;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.DoubleType;
import com.intel.hadoop.graphbuilder.types.FloatType;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * <p>
 * Represents a RDF mapping used in converting property graphs into RDF graphs
 * </p>
 * <h3>Mapping Format</h3>
 * <p>
 * Mappings are expressed as Pig maps, the mapping contains any number of the
 * supported keys all of which are optional e.g.
 * </p>
 * 
 * <pre>
 * [ 'base' # 'http://example.org/ontology#', 'idBase' # 'http://example.org/instances/',
 *   'namespaces' # [ 'ex' # 'http://example.org/base#' ],
 *   'useStdNamespaces' # 'true',
 *   'includedProperties' # ( 'name', 'age' ),
 *   'excludedProperties' # ( 'dob' ),
 *   'propertyMap' # [ 'type' # 'rdf:type', 'name' # 'http://xmlns.com/foaf/0.1/name' ],
 *   'idProperty' # 'ssn' ]
 * </pre>
 * 
 * <h4>base</h4>
 * <p>
 * The {@code base} key provides a Base URI which is used to resolve property
 * URIs against where properties aren't explicitly mapped to prefixed names or
 * absolute URIs. For example with the above mapping a property named
 * {@code name} yields the property URI {@code http://example.org/ontology#name}
 * . When this is not specified property URIs will be left as relative URIs.
 * </p>
 * <h4>idBase</h4>
 * <p>
 * The {@code idBase} key provides a Base URI which is used to resolve vertex
 * URIs against. For example with the above mapping a vertex with ID of
 * {@code 1} would yield the vertex URI {@code http://example.org/instances/1}.
 * When this is not specified the value of the {@code base} key is used if
 * possible and if not relative URIs are used.
 * </p>
 * <h4>namespaces</h4>
 * <p>
 * The {@code namespaces} key provides a map of namespace definitions which can
 * then be used with the {@code propertyMap} key to provide simple mappings of
 * property names into specific URIs.
 * </p>
 * <h4>useStdNamespaces</h4>
 * <p>
 * The {@code useStdNamespaces} key provides a {@code true} or {@code false}
 * value indicating whether standard namespaces are made available. Currently
 * these are {@code rdf}, {@code rdfs}, {@code owl} and {@code xsd}. Even when
 * enabled users can override the standard mappings by using the
 * {@code namespaces} key.
 * </p>
 * <h4>includedProperties</h4>
 * <p>
 * The {@code includedProperties} key provides a tuple of property names that
 * are included in the mapping to RDF. When specified a property must not also
 * be excluded to be included in the mapping, when not specified all properties
 * are assumed to be included unless excluded using the
 * {@code excludedProperties} key.
 * </p>
 * <h4>excludedProperties</h4>
 * <p>
 * The {@code excludedProperties} key provides a tuple of property names that
 * are excluded from the mapping to RDF. When specified exclusions take
 * precedence over any inclusions that may have been specified, when not
 * specified no properties are explicitly included though the
 * {@code includedProperties} key may be used to limit properties included in
 * the mapping.
 * </p>
 * <h4>propertyMap</h4>
 * <p>
 * The {@code propertyMap} key provides a map that is used to customize how
 * property names and edge labels are translated into property URIs in the RDF.
 * The key is the property or edge label whose translation is to be customized
 * and the value is either a Prefixed Name or URI. Prefixed Names may refer to
 * namespaces declared either via the {@code namespaces} key or from the
 * standard namespaces (where included) as shown in the example mapping.
 * </p>
 * <p>
 * In the example mapping the {@code type} property is mapped to
 * {@code rdf:type} which is a prefixed name while the {@code name} property is
 * mapped to the URI {@code http://xmlns.com/foaf/0.1}.
 * </p>
 * <h4>idProperty</h4>
 * <p>
 * The {@code idProperty} key provides a property that will be used to associate
 * the vertex ID as a literal value to the generated vertex URI as well as
 * embedding the vertex ID in that generated URI.
 * </p>
 */
public class RdfMapping extends AbstractMapping {

    protected static final String BASE_URI = "base";
    protected static final String ID_BASE_URI = "idBase";
    protected static final String NAMESPACES = "namespaces";
    protected static final String USE_STD_NAMESPACES = "useStdNamespaces";
    protected static final String INCLUDED_PROPERTIES = "includedProperties";
    protected static final String EXCLUDED_PROPERTIES = "excludedProperties";
    protected static final String PROPERTY_MAP = "propertyMap";
    protected static final String ID_PROPERTY = "idProperty";

    private String baseUri, idBaseUri, idProperty;
    private Set<String> includedProperties = new HashSet<String>();
    private Set<String> excludedProperties = new HashSet<String>();
    private Map<String, String> propertyMap = new HashMap<String, String>();
    private Map<String, String> namespaces = new HashMap<String, String>();
    private boolean useStdNamespaces = false;
    private PrefixMap prefixes;

    /**
     * Creates a new RDF Mapping
     * 
     * @param baseUri
     *            Base URI
     * @param idBaseUri
     *            ID Base URI
     * @param namespaces
     *            Namespaces
     * @param useStdNamespaces
     *            Whether to use standard namespaces
     * @param includedProperties
     *            Included properties
     * @param excludedProperties
     *            Excluded properties
     * @param propertyMap
     *            Property Mapping
     * @param idProperty
     *            ID Property
     */
    public RdfMapping(String baseUri, String idBaseUri, Map<String, String> namespaces, boolean useStdNamespaces,
            Collection<String> includedProperties, Collection<String> excludedProperties, Map<String, String> propertyMap,
            String idProperty) {
        this.baseUri = baseUri;
        this.idBaseUri = idBaseUri;
        if (includedProperties != null)
            this.includedProperties.addAll(includedProperties);
        if (excludedProperties != null)
            this.excludedProperties.addAll(excludedProperties);
        if (propertyMap != null)
            this.propertyMap.putAll(propertyMap);
        this.idProperty = idProperty;

        // Build the prefix map
        this.prefixes = this.useStdNamespaces ? PrefixMapFactory.create(PrefixMapping.Standard) : PrefixMapFactory.create();
        if (namespaces != null) {
            this.namespaces.putAll(namespaces);
            this.prefixes.putAll(namespaces);
        }
    }

    /**
     * Creates a new RDF mapping directly from an Object
     * <p>
     * This constructor assumes that the passed object comes from the processing
     * of a Pig script and thus will be a Map generated from Pig. See the
     * documentation for {@link RdfMapping} for details of the map format
     * expected.
     * <p>
     * 
     * @param object
     * @throws ExecException
     */
    @SuppressWarnings("unchecked")
    public RdfMapping(Object object) throws ExecException {
        if (object == null)
            throw new NullPointerException("Cannot create an edge mapping from a null object");
        if (!(object instanceof Map<?, ?>))
            throw new IllegalArgumentException("Cannot create an edge mapping from a non-map object");

        Map<String, Object> rdfMapping = (Map<String, Object>) object;
        this.baseUri = this.getStringValue(rdfMapping, BASE_URI, false);
        this.idBaseUri = this.getStringValue(rdfMapping, ID_BASE_URI, false);
        List<String> includes = this.getListValue(rdfMapping, INCLUDED_PROPERTIES, false);
        if (includes != null)
            this.includedProperties.addAll(includes);
        List<String> excludes = this.getListValue(rdfMapping, EXCLUDED_PROPERTIES, false);
        if (excludes != null)
            this.excludedProperties.addAll(excludes);
        Map<String, String> pmap = this.getTypedMapValue(rdfMapping, PROPERTY_MAP, false);
        if (pmap != null)
            this.propertyMap.putAll(pmap);
        this.idProperty = this.getStringValue(rdfMapping, ID_PROPERTY, false);

        this.useStdNamespaces = this.getBooleanValue(rdfMapping, USE_STD_NAMESPACES, false);
        this.prefixes = this.useStdNamespaces ? PrefixMapFactory.create(PrefixMapping.Standard) : PrefixMapFactory.create();
        Map<String, String> namespaces = this.getTypedMapValue(rdfMapping, NAMESPACES, false);
        if (namespaces != null)
            this.prefixes.putAll(namespaces);
    }

    /**
     * Gets the Base URI
     * <p>
     * This is used as the Base URI to generate property URIs which are not
     * otherwise mapped to specific URIs. Property URIs are generated simply by
     * prepending this Base URI to the property name.
     * </p>
     * <p>
     * In the event that this is not specified then property URIs are left as
     * relative URIs.
     * </p>
     * 
     * @return Base URI
     */
    public String getBaseUri() {
        return this.baseUri;
    }

    /**
     * Gets the ID Base URI
     * <p>
     * This differs from the {@link #getBaseUri()} in that this is used
     * exclusively for prepending to vertex IDs to produce the URIs for
     * vertices.
     * </p>
     * <p>
     * In the event that this is not specified then the normal Base URI is used
     * instead, if that is also not specified then vertex URIs are left as
     * relative URIs.
     * <p>
     * 
     * @return ID Base URI
     */
    public String getIdBaseUri() {
        return this.idBaseUri;
    }

    /**
     * Gets a prefix map containing the available namespace declarations
     * <p>
     * Depending on whether {@link #usingStandardNamespaces()} is true this may
     * be a combination of the user supplied namespaces and the standard
     * namespaces
     * <p>
     * 
     * @return Prefix map
     */
    public PrefixMap getNamespaces() {
        return this.prefixes;
    }

    /**
     * Gets whether standard namespaces are being used in addition to any user
     * defined ones
     * <p>
     * Standard namespaces are supplied by {@link PrefixMapping#Standard} which
     * contains RDF, RDFS, XSD, OWL and DC
     * </p>
     * 
     * @return True if using standard namespaces, false otherwise
     */
    public boolean usingStandardNamespaces() {
        return this.useStdNamespaces;
    }

    /**
     * Gets whether a given property is included in the mapping.
     * <p>
     * Whether a property is included is determined by checking several
     * conditions. Firstly it looks at whether the mapping explicitly
     * includes/excludes properties, if it does not then the property is
     * included. However if there are explicit includes/excludes then it has to
     * inspect these to determine if the property is included. A property must
     * be in the includes list (which if empty implicitly includes everything)
     * and not also in the exclude list (which if empty is ignored).
     * </p>
     * 
     * @param property
     *            Property
     * @return True if the property is included in the mapping, false otherwise
     */
    public boolean includesProperty(String property) {
        if (this.includedProperties.size() == 0 && this.excludedProperties.size() == 0) {
            return true;
        } else {
            if (this.includedProperties.size() > 0) {
                // Must be included and not excluded
                if (!this.includedProperties.contains(property) || this.excludedProperties.contains(property))
                    return false;
                return true;
            } else {
                // Implicitly all properties are included so must not be
                // excluded
                return !this.excludedProperties.contains(property);
            }
        }
    }

    /**
     * Gets the URI for a given vertex
     * <p>
     * See the documentation on {@link #getIdBaseUri()} to see how this is
     * calculated.
     * </p>
     * 
     * @param vertex
     *            Vertex ID
     * @return Vertex URI
     */
    public String getVertexUri(String vertex) {
        if (this.idBaseUri != null) {
            return this.idBaseUri + vertex;
        } else if (this.baseUri != null) {
            return this.baseUri + vertex;
        } else {
            return vertex;
        }
    }

    /**
     * Gets the URI for a given property
     * <p>
     * Determining the URI for a property is a multi-stage process which takes
     * account of several settings in the mapping. Firstly it checks to see that
     * the property in question is actually included in the mapping and if not
     * returns {@code null}.
     * </p>
     * <p>
     * If it is included it then looks in the property mapping since any
     * property may optionally be mapped to a URI reference. If it is mapped it
     * then has to resolve that URI reference by either expanding the prefixed
     * name or using the URI as is. When using the URI as-is it will attempt to
     * resolve it against the Base URI if the initial URI is not absolute.
     * </p>
     * <p>
     * If the property is not explicitly mapped then the property name will be
     * converted into a URI by appending it to the Base URI (assuming there is
     * one). In the event that there is no Base URI then the property URI will
     * remain relative.
     * </p>
     * 
     * @param property
     *            Property
     * @return Property URI or null if the property should not be included in
     *         the RDF output
     */
    public String getPropertyUri(String property) {
        // Ignore properties which aren't included
        if (!this.includesProperty(property))
            return null;

        // Is the property explicitly mapped?
        if (this.propertyMap.containsKey(property)) {
            String uriref = this.propertyMap.get(property);
            return this.resolveUriReference(uriref);
        }

        // Otherwise resolve against the Base URI
        return this.resolveUri(property);
    }

    /**
     * Gets the ID property (if any)
     * <p>
     * The ID property allows for associating the vertex ID as a literal value
     * to the generated vertex URI in addition to embedding it into the URI.
     * <p>
     * 
     * @return ID property
     */
    public String getIdProperty() {
        return this.idProperty;
    }

    /**
     * Resolves a URI reference
     * 
     * @param uriref
     *            URI Reference, may be a prefixed name or a relative/absolute
     *            URI
     * @return URI
     */
    private String resolveUriReference(String uriref) {
        if (uriref == null)
            return null;

        // Allow the special a shortcut to refer to rdf:type predicate
        if (uriref.equals("a"))
            return RDF.type.getURI();

        // Then try to resolve as prefixed name
        if (uriref.contains(":")) {
            String[] parts = uriref.split(":");
            String nsPrefix = parts[0];
            String localName = uriref.substring(nsPrefix.length() + 1);

            if (this.prefixes.contains(nsPrefix)) {
                return this.prefixes.expand(nsPrefix, localName);
            }
        }

        // Otherwise try to resolve as URI
        return this.resolveUri(uriref);
    }

    /**
     * Resolves a URI
     * 
     * @param uri
     *            URI, may be relative or absolute
     * @return Resolved URI
     */
    private String resolveUri(String uri) {
        return this.resolveUri(uri, this.baseUri);
    }

    private String resolveUri(String uri, String baseUri) {
        IRI iri = IRIFactory.iriImplementation().create(uri);
        if (iri.isAbsolute()) {
            // Already an absolute URI
            return uri;
        } else if (baseUri != null) {
            // Attempt to resolve against Base URI
            return IRIResolver.resolveString(uri, baseUri);
        } else if (iri.hasViolation(false)) {
            // Check for illegal URI after trying to relativize because
            // that may succeed and relativization will error if it fails
            // anyway
            throw new IllegalArgumentException("URI " + uri + " is an illegal IRI");
        } else {
            // Leave as a relative URI
            return uri;
        }
    }

    /**
     * Applies the mapping to the given input
     * 
     * @param input
     *            Input
     * @param output
     *            Output
     * @throws ExecException
     */
    @SuppressWarnings("unchecked")
    public void apply(Tuple input, DataBag output) throws ExecException {
        if (input == null || input.size() != 2)
            return;
        SerializedGraphElementStringTypeVids element = (SerializedGraphElementStringTypeVids) input.get(0);
        GraphElement<StringType> graphElement = element.graphElement();
        if (graphElement == null)
            return;
        if (graphElement.isEdge()) {
            // Edge Mapping

            // Get the predicate URI
            Edge<StringType> edge = (Edge<StringType>) graphElement.get();
            String predicateUri = this.getPropertyUri(edge.getLabel().get());
            if (predicateUri == null)
                return;
            String sourceId = edge.getSrc().getName().get();
            String targetId = edge.getDst().getName().get();
            if (sourceId == null || targetId == null)
                return;

            // Generate the triple for the edge
            Triple edgeTriple = new Triple(NodeFactory.createURI(this.getVertexUri(sourceId)),
                    NodeFactory.createURI(predicateUri), NodeFactory.createURI(this.getVertexUri(targetId)));

            // Output the triple
            this.outputTriple(edgeTriple, output);
        } else if (element.graphElement().isVertex()) {
            // Vertex Mapping

            // Get the vertex URI
            Vertex<StringType> vertex = (Vertex<StringType>) graphElement.get();
            String subjectUri = this.getVertexUri(vertex.getId().getName().get());
            if (subjectUri == null)
                return;
            Node subject = NodeFactory.createURI(subjectUri);

            // Add ID Property if this is mapped
            if (this.idProperty != null) {
                String idUri = this.getPropertyUri(this.idProperty);
                if (idUri != null) {
                    this.outputTriple(
                            new Triple(subject, NodeFactory.createURI(idUri), NodeFactory.createLiteral(vertex.getId().getName()
                                    .get())), output);
                }
            }

            // Add all relevant properties
            for (Writable property : vertex.getProperties().getPropertyKeys()) {
                String propertyName = ((StringType) property).get();
                String propertyUri = this.getPropertyUri(propertyName);
                if (propertyUri == null)
                    continue;
                Node object = this.toObject(vertex.getProperties().getProperty(propertyName));
                if (object == null)
                    continue;

                this.outputTriple(new Triple(subject, NodeFactory.createURI(propertyUri), object), output);
            }
        }
    }

    private Node toObject(Writable value) {
        // Since the RDF Mapping must apply to something generated by the
        // Property Graph Mapping we know that there should in principal only be
        // a limited range of types we need to convert
        if (value instanceof StringType) {
            return NodeFactory.createLiteral(((StringType) value).get());
        } else if (value instanceof IntType) {
            return NodeFactoryExtra.intToNode(((IntType) value).get());
        } else if (value instanceof LongType) {
            return NodeFactoryExtra.intToNode(((LongType) value).get());
        } else if (value instanceof FloatType) {
            return NodeFactoryExtra.floatToNode(((FloatType) value).get());
        } else if (value instanceof DoubleType) {
            return NodeFactoryExtra.doubleToNode(((DoubleType) value).get());
        } else {
            // Can't convert other types
            return null;
        }
    }

    /**
     * Outputs a triple
     * 
     * @param t
     *            Triple
     * @param output
     *            Output
     */
    private void outputTriple(Triple t, DataBag output) {
        StringBuilder tripleString = new StringBuilder();
        tripleString.append(FmtUtils.stringForTriple(t, null));
        tripleString.append(" .");
        output.add(TupleFactory.getInstance().newTuple(tripleString.toString()));
    }

    @Override
    public Map<String, Object> toMap() throws ExecException {
        Map<String, Object> mapping = new HashMap<String, Object>();
        if (this.baseUri != null)
            mapping.put(BASE_URI, this.baseUri);
        if (this.idBaseUri != null)
            mapping.put(ID_BASE_URI, this.idBaseUri);
        if (this.idProperty != null)
            mapping.put(ID_PROPERTY, this.idProperty);
        mapping.put(USE_STD_NAMESPACES, Boolean.toString(this.useStdNamespaces).toLowerCase());
        if (this.namespaces.size() > 0)
            mapping.put(NAMESPACES, this.namespaces);
        if (this.includedProperties.size() > 0)
            mapping.put(INCLUDED_PROPERTIES, this.setToTuple(this.includedProperties));
        if (this.excludedProperties.size() > 0)
            mapping.put(EXCLUDED_PROPERTIES, this.setToTuple(this.excludedProperties));
        if (this.propertyMap.size() > 0)
            mapping.put(PROPERTY_MAP, this.propertyMap);
        return mapping;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        Map<String, String> properties = new HashMap<>();
        if (this.baseUri != null)
            properties.put(BASE_URI, this.baseUri);
        if (this.idBaseUri != null)
            properties.put(ID_BASE_URI, this.idBaseUri);
        if (this.idProperty != null)
            properties.put(ID_PROPERTY, this.idProperty);
        properties.put(USE_STD_NAMESPACES, Boolean.toString(this.useStdNamespaces).toLowerCase());

        Iterator<Entry<String, String>> es = properties.entrySet().iterator();
        while (es.hasNext()) {
            Entry<String, String> e = es.next();
            builder.append('\'');
            builder.append(e.getKey());
            builder.append("' # '");
            builder.append(e.getValue());
            builder.append('\'');
            if (es.hasNext())
                builder.append(',');
            builder.append(' ');
        }

        if (this.includedProperties.size() > 0) {
            builder.append(", ");
            builder.append(this.tupleToMapKeyValueString(this.includedProperties, INCLUDED_PROPERTIES));
        }
        if (this.excludedProperties.size() > 0) {
            builder.append(", ");
            builder.append(this.tupleToMapKeyValueString(this.excludedProperties, EXCLUDED_PROPERTIES));
        }
        if (this.namespaces.size() > 0) {
            builder.append(", ");
            builder.append(this.mapToMapKeyValueString(this.namespaces, NAMESPACES));
        }
        if (this.propertyMap.size() > 0) {
            builder.append(", ");
            builder.append(this.mapToMapKeyValueString(this.propertyMap, PROPERTY_MAP));
        }

        builder.append(']');
        return builder.toString();
    }
}
