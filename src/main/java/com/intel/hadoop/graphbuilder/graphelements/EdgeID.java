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
package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.util.Triple;

/**
 * The identifying information of an edge: Its source, its destination, and its label.
 *
 * <p>
 * This class is used in the initial reducer of an {@code GraphGenerationJob} to detect 
 * and handle duplicate edges. Edges are considered duplicate if they have identical source, 
 * destination, and label. Property maps are <i>not</i> used for purposes of comparison.
 *    </p>
 * @see com.intel.hadoop.graphbuilder.pipeline.output.titan.EdgesIntoTitanReducer
 */
public class EdgeID {

    private Triple<Object, Object, Object> triple = null;


    /**
     * Construct {@code EdgeID} from parameters.
     * @param {@code src}    The source vertex of the edge.
     * @param {@code dst}    The destination vertex of the edge.
     * @param {@code label}  The label of the edge.
     */
    public EdgeID(Object src, Object dst, Object label) {
        triple = new Triple(src, dst, label);
    }

    /**
     * @return  The source vertex of the edge.
     */
    public Object getSrc() {
        return triple.getA();
    }

    /**
     * @return  The destination vertex of the edge.
     */
    public Object getDst() {
        return triple.getB();
    }

    /**
     * @return  The label of the edge.
     */
    public Object getLabel() {
        return triple.getC();
    }

    /**
     * @param {@code val}  The new value for EdgeID's source vertex.
     */
    public void setSrc(Object val) {
        triple.setA(val);
    }

    /**
     * @param {@code val}  The new value for EdgeID's destination vertex.
     */
    public void setDst(Object val) {
        triple.setB(val);
    }

    /**
     * @param {@code val} The new value for EdgeID's label.
     */
    public void setLabel(Object val) {
        triple.setC(val);
    }

    /**
     * Reverses the direction of a given edge.
     * @return A new edge, whose source is the destination of the base edge, 
     * and whose label is the same label as the base edge.
     */
    public EdgeID reverseEdge() {
        return new EdgeID(this.getDst(), this.getSrc(), this.getLabel());
    }

    /**
     * Equality test: Is the other object an {@code EdgeID} whose components are the same objects as this one?
     * @param {@code obj}  Any object.
     * @return  A triple ...
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EdgeID) {
            EdgeID k = (EdgeID) obj;
            return this.triple.equals(k.triple);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return triple.hashCode();
    }

    @Override
    public String toString() {
        return triple.toString();
    }
}
