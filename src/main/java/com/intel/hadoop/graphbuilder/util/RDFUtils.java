/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
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
package com.intel.hadoop.graphbuilder.util;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.*;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import org.apache.hadoop.io.Writable;

import java.util.Hashtable;
import java.util.Map;

public class RDFUtils {

	public static final Map<String, String> RDFNamespaceMap;
	static {
		RDFNamespaceMap = new Hashtable<String, String>();
        RDFNamespaceMap.put("DB",         DB.getURI());
        RDFNamespaceMap.put("DC",         DC.NS);
        RDFNamespaceMap.put("LOCMAP",     LocationMappingVocab.NS);
        RDFNamespaceMap.put("ONTDOC",     OntDocManagerVocab.NS);
        RDFNamespaceMap.put("ONTEVENTS",  OntDocManagerVocab.NS);
        RDFNamespaceMap.put("OWL",        OWL.NS);
        RDFNamespaceMap.put("OWL2",       OWL2.NS);
        RDFNamespaceMap.put("RDF",        RDF.getURI());
        RDFNamespaceMap.put("RDFS",       RDFS.getURI());
        RDFNamespaceMap.put("RDFSYNTAX",  RDFSyntax.getURI());
        RDFNamespaceMap.put("RSS",        RSS.getURI());
        RDFNamespaceMap.put("VCARD",      VCARD.getURI());
        RDFNamespaceMap.put("XMLSchema",  XSD.getURI());
	}

	public static Resource createResourceFromVertex(String rdfNamespace,
                                                    Vertex vertex) {

		// Namespace can be DB, DC, LOCMAP, ONTDOC, ONTEVENTS, OWL, OWL2,
        // RDF, RDFS, RDFSYNTAX, RSS, VCARD or XSD

        String label              = vertex.getLabel().toString();
        String vertexKey          = vertex.getId().getName().toString();
        PropertyMap vertexPropMap = vertex.getProperties();
		String namespace          = null;
        String vertexType         = null;

        // Each vertex and edge type should be associated with a namespace
        // and the same namespace will be used for all its properties. If a
        // namespace is not specified, then the global namespace specified
        // with the '-n' command line parameter will be used.
        // Extra logic needs to be written to reify vertex or edge properties
        // separately

        if (label.contains(".")) {
            String [] tempArray = label.split("\\.");
            namespace  = RDFUtils.RDFNamespaceMap.get(tempArray[0]);
            vertexType = tempArray[1];
        } else {
            namespace  = RDFUtils.RDFNamespaceMap.get(rdfNamespace);
            vertexType = label;
        }

		// create an empty Model
		Model model = ModelFactory.createDefaultModel();

		// create the resource
		Resource vertexRdf = model.createResource(namespace + vertexKey);

		if (vertexType != null && !vertexType.isEmpty()) {
            Property vertexTypeProperty =
                    model.getProperty(RDF.type.toString());
            vertexRdf.addProperty(vertexTypeProperty, vertexType);
        }

        for (Writable property : vertexPropMap.getPropertyKeys()) {
            Property vertexRDFProperty =
                    model.getProperty(namespace + property.toString());
            Literal propertyValue = model.createLiteral(
                    vertexPropMap.getProperty(property.toString()).toString());
            vertexRdf.addLiteral(vertexRDFProperty, propertyValue);
        }

		return vertexRdf;
	}

	public static Resource createResourceFromEdge(String rdfNamespace,
                                                  Edge edge) {
	//		String source, String target, String edgeLabel,
	//		PropertyMap edgePropertyMap) {

		// Namespace can be DB, DC, LOCMAP, ONTDOC, ONTEVENTS, OWL, OWL2,
        // RDF, RDFS, RDFSYNTAX, RSS, VCARD or XSD

        String edgeLabel          = edge.getLabel().toString();
        String sourceVertex       = edge.getSrc().getName().toString();
        String targetVertex       = edge.getDst().getName().toString();
		String namespace          = null;
        String edgeType = null;

        // Each edge type should be associated with a namespace
        // and the same namespace will be used for all its properties. If a
        // namespace is not specified, then the global namespace specified
        // with the '-n' command line parameter will be used.
        // Extra logic needs to be written to reify vertex or edge properties
        // separately

        if (edgeLabel.contains(".")) {
            String [] tempArray = edgeLabel.split("\\.");
            namespace  = RDFUtils.RDFNamespaceMap.get(tempArray[0]);
            edgeType = tempArray[1];
        } else {
            namespace  = RDFUtils.RDFNamespaceMap.get(rdfNamespace);
            edgeType = edgeLabel;
        }

		// create an empty Model

        Model model = ModelFactory.createDefaultModel();

        // create the edge triple
        // edge properties are ignored in this release

        Resource sourceVertexRdf = model.createResource(namespace +
                sourceVertex);
        Resource targetVertexRdf = model.createResource(namespace +
                targetVertex);
        Property edgeRDFLabel = model.createProperty(namespace, edgeType);

        sourceVertexRdf.addProperty(edgeRDFLabel, targetVertexRdf);
		return sourceVertexRdf;
	}
}
