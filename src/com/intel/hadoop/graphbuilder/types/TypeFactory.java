/* Copyright (C) 2012 Intel Corporation.
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
package com.intel.hadoop.graphbuilder.types;

import javassist.NotFoundException;

public class TypeFactory {
  public static String getConstructorCode(String classname) {
    if (classname.contains("EmptyType")) {
      return "return " + classname + ".INSTANCE;";
    } else {
      return "return new " + classname + "();";
    }
  }

  public static String getClassName(String typename) throws NotFoundException {
    if (typename.toLowerCase().equals("string")) {
      return StringType.class.getName();
    } else if (typename.toLowerCase().equals("int")) {
      return IntType.class.getName();
    } else if (typename.toLowerCase().equals("long")) {
      return LongType.class.getName();
    } else if (typename.toLowerCase().equals("float")) {
      return FloatType.class.getName();
    } else if (typename.toLowerCase().equals("double")) {
      return DoubleType.class.getName();
    } else if (typename.toLowerCase().equals("none")) {
      return EmptyType.class.getName();
    } else {
      throw new NotFoundException("Type " + typename + " cannot be found.");
    }
  }
}
