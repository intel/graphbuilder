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
package com.intel.hadoop.graphbuilder.parser;

import javassist.NotFoundException;

/**
 * Factory for getting parser of a given type.
 * 
 */
public class ParserFactory {
  /**
   * @param typename
   * @return FieldParser for the type.
   * @throws Exception
   */
  public static FieldParser getParserByTypeName(String typename)
      throws NotFoundException {
    if (typename.toLowerCase().equals("int")) {
      return new IntParser();
    } else if (typename.toLowerCase().equals("long")) {
      return new LongParser();
    } else if (typename.toLowerCase().equals("none")) {
      return new EmptyParser();
    } else if (typename.toLowerCase().equals("float")) {
      return new FloatParser();
    } else if (typename.toLowerCase().equals("string")) {
      return new StringParser();
    } else {
      throw new NotFoundException("Parser of type: " + typename
          + " is not supported");
    }
  }

  public static FieldParser getParserByClassName(String classname)
      throws NotFoundException {
    if (classname.toLowerCase().endsWith("inttype")) {
      return new IntParser();
    } else if (classname.toLowerCase().endsWith("longtype")) {
      return new LongParser();
    } else if (classname.toLowerCase().endsWith("emptytype")) {
      return new EmptyParser();
    } else if (classname.toLowerCase().endsWith("floattype")) {
      return new FloatParser();
    } else if (classname.toLowerCase().endsWith("stringtype")) {
      return new StringParser();
    } else {
      throw new NotFoundException("Parser of type: " + classname
          + " is not supported");
    }
  }
}
