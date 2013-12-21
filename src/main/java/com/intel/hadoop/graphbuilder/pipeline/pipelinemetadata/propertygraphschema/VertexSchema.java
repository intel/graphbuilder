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
package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import java.util.ArrayList;

/**
 * The type of a vertex declaration used in graph construction. It encapsulates
 * the names and datatypes of the properties that can be associated with vertices
 * of this type.
 *
 * The expected use of this information is declaring keys for loading the 
 * constructed graph into a graph database.
 */

public class VertexSchema {

    private ArrayList<PropertySchema> propertySchemata;

    public VertexSchema() {
        propertySchemata = new ArrayList<PropertySchema>();
    }

    public ArrayList<PropertySchema> getPropertySchemata() {
        return propertySchemata;
    }

}
