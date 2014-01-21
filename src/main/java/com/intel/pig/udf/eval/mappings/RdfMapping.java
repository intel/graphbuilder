/*
 * Copyright 2014 YarcData LLC All Rights Reserved.
 */

package com.intel.pig.udf.eval.mappings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.riot.system.PrefixMap;

/**
 * Represents a RDF mapping used in converting property graphs into RDF graphs
 * 
 */
public class RdfMapping {

    private String baseUri, idBaseUri;
    private Map<String, String> namespaces = new HashMap<String, String>();
    private Set<String> includedProperties = new HashSet<String>();
    private Set<String> excludedProperties = new HashSet<String>();
    private Map<String, String> propertyMap = new HashMap<String, String>();
    private boolean useStdNamespaces = false;

    public String getBaseUri() {
        return this.baseUri;
    }

    public String getIdBaseUri() {
        return this.idBaseUri;
    }

    /**
     * Gets a prefix map containing the available namespace declarations
     * <p>
     * Depending on whether {@link #usingStandardNamespaces()} is true this may
     * be
     * <p>
     * 
     * @return Prefix map
     */
    public PrefixMap getNamespaces() {
        // TODO Generate a prefix map adding in standard namespaces if
        // applicable
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Gets whether standard namespaces are being used in addition to any user
     * defined ones
     * 
     * @return True if using standard namespaces, false otherwise
     */
    public boolean usingStandardNamespaces() {
        return this.useStdNamespaces;
    }

}
