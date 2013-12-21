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
package com.intel.pig.data;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.List;

/**
 * \brief TupleFactory to create tuples of type
 * {@link PropertyGraphElementTuple}
 */
public class GBTupleFactory extends TupleFactory {

	/**
	 * Creates a new empty {@link PropertyGraphElementTuple}
	 * 
	 * @return a empty {@link PropertyGraphElementTuple}
	 */
	@Override
	public Tuple newTuple() {
		return new PropertyGraphElementTuple();
	}

	/**
	 * Creates a new {@link PropertyGraphElementTuple} of size <code>size</code>
	 * 
	 * @param the
	 *            size of the tuple
	 * @return a {@link PropertyGraphElementTuple} of the given size
	 */
	@Override
	public Tuple newTuple(int size) {
		return new PropertyGraphElementTuple(size);
	}

	/**
	 * Creates a new {@link PropertyGraphElementTuple} with the given list of
	 * {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s
	 * 
	 * @param the
	 *            list of {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s
	 * @return a {@link PropertyGraphElementTuple} containing the list of given
	 *         {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s
	 */
	@Override
	public Tuple newTuple(List list) {
		return new PropertyGraphElementTuple(list);
	}

	/**
	 * Creates a new {@link PropertyGraphElementTuple} with the given list of
	 * {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s
	 * 
	 * @param the
	 *            list of {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s
	 * @return a {@link PropertyGraphElementTuple} containing the list of given
	 *         {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s
	 */
	@Override
	public Tuple newTupleNoCopy(List list) {
		return new PropertyGraphElementTuple(list);
	}

	/**
	 * Create a tuple with a single {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}
	 * 
	 * @param pge
	 *            {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement} to put in the tuple
	 * @return A {@link PropertyGraphElementTuple} with one
	 *         {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}
	 */
	@Override
	public Tuple newTuple(Object pge) {
		Tuple t = new PropertyGraphElementTuple(1);
		try {
			t.set(0, pge);
		} catch (ExecException e) {
			throw new RuntimeException("Unable to write to field 0 in newly "
					+ "allocated tuple of size 1!", e);
		}
		return t;
	}

	/**
	 * Return the tuple type this tuple factory creates
	 * 
	 * @return the tuple class created by this tuple factory
	 */
	@Override
	public Class<? extends Tuple> tupleClass() {
		return PropertyGraphElementTuple.class;
	}

	/**
	 * This method is used to inspect whether the Tuples created by this factory
	 * will be of a fixed size when they are created. In practical terms, this
	 * means whether they support append or not.
	 * 
	 * @return whether the Tuple is fixed or not
	 */
	@Override
	public boolean isFixedSize() {
		return false;
	}
}
