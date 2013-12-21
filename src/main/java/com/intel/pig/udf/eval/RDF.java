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
package com.intel.pig.udf.eval;

import com.hp.hpl.jena.rdf.model.*;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.util.RDFUtils;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import java.io.IOException;

/**
 * \brief RDF UDF converts a given {@link PropertyGraphElementTuple} to a bag of
 * RDF statements.
 * <p/>
 * If the
 * {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement} is
 * null, this UDF returns null.
 * 
 * <b>Example:</b>
 * 
 * <pre>
 * {@code
          DEFINE RDF com.intel.pig.udf.eval.RDF('OWL');--specify the namespace to use with the constructor
          DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements2('-v "[OWL.People],id=name,age,dept" "[OWL.People],manager" -e "id,manager,OWL.worksUnder,underManager"');
          x = LOAD 'examples/data/employees.csv' USING PigStorage(',') as (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
          x = FILTER x by id!='';--remove employee records with missing ids
          pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));--create the property graph elements from raw source data
          rdf_triples = FOREACH pge GENERATE FLATTEN(RDF(*));--create RDF tuples from the property graph elements
          STORE rdf_triples INTO '/tmp/rdf_triples' USING PigStorage();
  }
 * </pre>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class RDF extends EvalFunc<DataBag> {
	private String rdfNamespace;
	private static  BagFactory bagFactory = DefaultBagFactory.getInstance();

	public RDF(String rdfNamespace) {
		this.rdfNamespace = rdfNamespace;
	}

	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag rdfBag = bagFactory.newDefaultBag();
		SerializedGraphElement e = (SerializedGraphElement) input.get(0);

		if (e == null) {
			warn("Null property graph element", PigWarning.UDF_WARNING_1);
			return null;
		}

		Resource resource = null;

		GraphElement graphElement = e.graphElement();

		if (graphElement == null) {
			warn("Null property graph element", PigWarning.UDF_WARNING_1);
			return null;
		}

		if (graphElement.isEdge()) {

			// create a Resource from the edge
			resource = RDFUtils.createResourceFromEdge(rdfNamespace,
                    (Edge) graphElement);

		} else if (graphElement.isVertex()) {

			// create a Resource from the vertex
			resource = RDFUtils.createResourceFromVertex(rdfNamespace,
                    (Vertex) graphElement);
		}

		/* create the RDF statements from the model */
		StmtIterator iterator = resource.getModel().listStatements();
		while (iterator.hasNext()) {
			Statement stmt = iterator.nextStatement();
			Resource subject = stmt.getSubject();
			Property predicate = stmt.getPredicate();
			RDFNode object = stmt.getObject();
			Tuple rdfTuple = TupleFactory.getInstance().newTuple(1);
			String rdfTripleAsString = subject.toString() + " "
					+ predicate.toString() + " " + object.toString() + " .";

			rdfTuple.set(0, rdfTripleAsString);
			rdfBag.add(rdfTuple);
		}
		return rdfBag;
	}

	/**
	 * RDF UDF returns a bag of RDF statements.
	 */
	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema rdfStatementTuple = new Schema(new Schema.FieldSchema(
					"rdf_statement", DataType.CHARARRAY));
			return new Schema(new FieldSchema("rdf_statements",
					rdfStatementTuple, DataType.BAG));
		} catch (FrontendException e) {
			throw new RuntimeException("Exception while "
					+ "creating output schema for RDF udf", e);
		}
	}
}
