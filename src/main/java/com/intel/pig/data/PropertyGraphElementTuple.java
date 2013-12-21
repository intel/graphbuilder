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

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.util.HashUtil;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * \brief PropertyGraphElementTuple is the tuple type processed by the GB 2.0
 * (alpha) dataflow
 * 
 * PropertyGraphElementTuple contains a list of property graph elements.
 * Currently, GB UDFs know how to process tuples of type
 * PropertyGraphElementTuple.
 */
public class PropertyGraphElementTuple extends AbstractTuple {

	List<SerializedGraphElement> serializedGraphElements;

	/**
	 * Constructs a PropertyGraphElementTuple with zero elements.
	 */
	public PropertyGraphElementTuple() {
		serializedGraphElements = new ArrayList<SerializedGraphElement>();
	}

	/**
	 * Constructs a PropertyGraphElementTuple with <code>size</code> elements
	 * and sets these elements to null
	 * 
	 * @param size
	 *            initial size of this tuple
	 * 
	 */
	public PropertyGraphElementTuple(int size) {
		serializedGraphElements = new ArrayList<SerializedGraphElement>(size);
		for (int i = 0; i < size; i++)
			serializedGraphElements.add(null);
	}

	/**
	 * Constructs a PropertyGraphElementTuple from a given list of
	 * {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s
	 * 
	 * @param elements
	 *            list of {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s
	 */
	public PropertyGraphElementTuple(List elements) {
		serializedGraphElements = elements;
	}

	/**
	 * Returns the number of {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s in this tuple.
	 * 
	 * @return the size of the list of {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement}s in this
	 *         tuple.
	 */
	@Override
	public int size() {
		return serializedGraphElements.size();
	}

	/**
	 * Get the {@link com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement} in the given <code>fieldNum</code>
	 * 
	 * @param fieldNum
	 *            Index of the SerializedGraphElement to get.
	 * @return the SerializedGraphElement as an Object.
	 * @throws ExecException
	 *             if the field number is greater than or equal to the number of
	 *             fields in the tuple.
	 */
	@Override
	public Object get(int fieldNum) throws ExecException {
		if (fieldNum >= serializedGraphElements.size()) {
			throw new ExecException(
					"Specified fieldNum "
							+ fieldNum
							+ " is greater than or equal to the number of fields in this tuple");
		}
		return serializedGraphElements.get(fieldNum);
	}

	/**
	 * Get all the PropertyGraphElements in this tuple as a list.
	 * 
	 * @return a list of objects containing the PropertyGraphElements of the
	 *         tuple in order.
	 */
	@Override
	public List<Object> getAll() {
		List<? extends Object> casted = serializedGraphElements;
		return (List<Object>) casted;
	}

	/**
	 * Set the value in a given field. This should not be called unless the
	 * tuple was constructed by {@link TupleFactory#newTuple(int)} with an
	 * argument greater than the fieldNum being passed here. This call will not
	 * automatically expand the tuple size. That is if you called
	 * {@link TupleFactory#newTuple(int)} with a 2, it is okay to call this
	 * function with a 1, but not with a 2 or greater.
	 * 
	 * @param fieldNum
	 *            Number of the field to set the value for.
	 * @param val
	 *            Object to put in the indicated field.
	 * @throws ExecException
	 *             if the field number is greater than or equal to the number of
	 *             fields in the tuple.
	 */
	@Override
	public void set(int fieldNum, Object val) throws ExecException {
		if (fieldNum >= serializedGraphElements.size()) {
			throw new ExecException(
					"Specified fieldNum "
							+ fieldNum
							+ " is greater than or equal to the number of fields in this tuple");
		}

		if (!(val instanceof SerializedGraphElement)) {
			throw new ExecException("Given value is of type "
					+ val.getClass().getName() + ". It should be of type "
					+ SerializedGraphElement.class.getName());
		}

		serializedGraphElements.set(fieldNum, (SerializedGraphElement) val);

	}

	/**
	 * Append a SerializedGraphElement to this tuple. This method is not efficient
	 * as it may force copying of existing data in order to grow the data
	 * structure. Whenever possible you should construct your Tuple with
	 * {@link TupleFactory#newTuple(int)} and then fill in the values with
	 * {@link #set(int, Object)}, rather than construct it with
	 * {@link TupleFactory#newTuple()} and append values.
	 * 
	 * @param val
	 *            Object to append to the tuple.
	 */
	@Override
	public void append(Object val) {
		if (!(val instanceof SerializedGraphElement)) {
			throw new RuntimeException("Given value is of type "
					+ val.getClass().getName() + ". It should be of type "
					+ SerializedGraphElement.class.getName());
		}
		serializedGraphElements.add((SerializedGraphElement) val);
	}

	/**
	 * This implementation is copied from {@link DefaultTuple} implementation
	 * 
	 * <br/>
	 * Determine the size of tuple in memory. This is used by data bags to
	 * determine their memory size. This need not be exact, but it should be a
	 * decent estimation.
	 * 
	 * @return estimated memory size, in bytes.
	 */
	@Override
	public long getMemorySize() {
		Iterator<SerializedGraphElement> i = serializedGraphElements.iterator();
		// fixed overhead
		long empty_tuple_size = 8 /* tuple object header */
		+ 8 /*
			 * isNull - but rounded to 8 bytes as total obj size needs to be
			 * multiple of 8
			 */
		+ 8 /* mFields reference */
		+ 32 /* mFields array list fixed size */;

		// rest of the fixed portion of mfields size is accounted within
		// empty_tuple_size
		long mfields_var_size = SizeUtil
				.roundToEight(4 + 4 * serializedGraphElements.size());
		// in java hotspot 32bit vm, there seems to be a minimum tuple size of
		// 96
		// which is probably from the minimum size of this array list
		mfields_var_size = Math.max(40, mfields_var_size);

		long sum = empty_tuple_size + mfields_var_size;
		while (i.hasNext()) {
			sum += SizeUtil.getPigObjMemSize(i.next());
		}
		return sum;
	}

	/**
	 * This implementation is copied from {@link DefaultTuple} implementation
	 * 
	 * <br/>
	 * Creates the list of PropertyGraphElements from the given binary stream
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// Clear our fields, in case we're being reused.
		serializedGraphElements.clear();
		// Make sure it's a tuple.
		byte b = in.readByte();
		if (b != DataType.TUPLE) {
			int errCode = 2112;
			String msg = "Unexpected data while reading tuple "
					+ "from binary file.";
			throw new ExecException(msg, errCode, PigException.BUG);
		}
		// Read the number of fields
		int sz = in.readInt();
		for (int i = 0; i < sz; i++) {
			try {
				append(DataReaderWriter.readDatum(in));
			} catch (ExecException ee) {
				throw ee;
			}
		}

	}

	/**
	 * This implementation is copied from {@link DefaultTuple} implementation
	 * 
	 * <br/>
	 * Writes the list of PropertyGraphElements to the given binary stream
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(DataType.TUPLE);
		int sz = size();
		out.writeInt(sz);
		for (int i = 0; i < sz; i++) {
			DataReaderWriter.writeDatum(out, serializedGraphElements.get(i));
		}
	}

	/**
	 * This method is implemented based on {@link DefaultTuple}
	 */
	@Override
	public int compareTo(Object other) {
		if (other instanceof PropertyGraphElementTuple) {
			PropertyGraphElementTuple t = (PropertyGraphElementTuple) other;
			int mySize = serializedGraphElements.size();
			int otherSize = t.size();
			/**
			 * Comparable requires us to return a negative integer, zero, or a
			 * positive integer if this object is less than, equal to, or
			 * greater than the specified object. We define the ordering based
			 * on tuple size.
			 */
			if (otherSize < mySize) {
				return 1;
			} else if (otherSize > mySize) {
				return -1;
			} else {
				for (int i = 0; i < mySize; i++) {
					SerializedGraphElement myPge = serializedGraphElements.get(i);
					SerializedGraphElement otherPge;
					try {
						otherPge = (SerializedGraphElement) t.get(i);
						int comparisonResult = myPge.compareTo(otherPge);
						/*
						 * if two PropertyGraphElements are not equal we are
						 * done
						 */
						if (comparisonResult != 0) {
							return comparisonResult;
						}
					} catch (ExecException e) {
						throw new RuntimeException("Unable to compare tuples",
								e);
					}
				}
				/* all PropertyGraphElements are equal */
				return 0;
			}
		}
		/**
		 * use Pig's default comparison logic
		 */
		return DataType.compare(this, other);
	}

    /**
     *
     * @return hash code of the property graph element tuple
     */
    public int hashCode() {
        int hash = 0;
        for (int i = 0; i < serializedGraphElements.size(); i++) {
            SerializedGraphElement pge = serializedGraphElements.get(i);
            hash = HashUtil.combine(hash, pge);
        }
        return hash;
    }
}
