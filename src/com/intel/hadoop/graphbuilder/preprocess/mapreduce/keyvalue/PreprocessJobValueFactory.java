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
package com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.Modifier;
import javassist.NotFoundException;

import com.intel.hadoop.graphbuilder.types.TypeFactory;
/**
 * A Factory class that dynamically creates a concrete {@code VertexEdgeUnionType}
 * which is used in {@code CreateGraphMR} as the intermediate value type.
 */
public class PreprocessJobValueFactory {
  private static ClassPool pool = ClassPool.getDefault();

  /**
   * 
   * @param vidClass
   * @param vdataClass
   * @param edataClass
   * @return a new Class which inherits {@code VertexEdgeUnionType} and
   * overrides {@code createVid}, {@code createVdata} and {@createEdata}
   * to return the input classes respectively.
   * @throws CannotCompileException
   * @throws NotFoundException
   */
  public static Class getValueClassByClassName(String vidClass,
      String vdataClass, String edataClass) throws CannotCompileException,
      NotFoundException {
    pool.insertClassPath(new ClassClassPath(PreprocessJobValueFactory.class));
    CtClass ctVal = pool.get(VertexEdgeUnionType.class.getName());
    ctVal.setName("generatedclass.MyPreprocessJobValue" + id);
    CtClass ctSuper = pool.get(VertexEdgeUnionType.class.getName());
    id++;
    ctVal.setSuperclass(ctSuper);
    ctVal.getDeclaredMethod("createVid").setBody(
        TypeFactory.getConstructorCode(vidClass));
    ctVal.getDeclaredMethod("createVdata").setBody(
        TypeFactory.getConstructorCode(vdataClass));
    ctVal.getDeclaredMethod("createEdata").setBody(
        TypeFactory.getConstructorCode(edataClass));
    ctVal.setModifiers(ctVal.getModifiers() & ~Modifier.ABSTRACT);
    CtField[] fields = ctVal.getFields();
    for (CtField f : fields) {
      if (!(f.getDeclaringClass().equals(ctSuper))
          && (f.getModifiers() & Modifier.PRIVATE) == 0)
        ctVal.removeField(f);
    }
    try {
      ctVal.writeFile();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ctVal.toClass();
  }

  private static int id;
}
