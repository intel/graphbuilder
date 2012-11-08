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
package com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.Modifier;
import javassist.NotFoundException;

import com.intel.hadoop.graphbuilder.types.TypeFactory;

public class IngressJobKeyValueFactory {
  private static ClassPool pool = ClassPool.getDefault();

  public static Class getKeyClassByClassName(String vidClassName)
      throws NotFoundException, CannotCompileException {
    pool.insertClassPath(new ClassClassPath(IngressJobKeyValueFactory.class));
    CtClass ctKey = pool.get(IngressKeyType.class.getName());
    ctKey.setName("generatedclass.MyIngressJobKey" + keyid);
    keyid++;
    CtClass ctSuper = pool.get(IngressKeyType.class.getName());
    ctKey.setSuperclass(ctSuper);
    ctKey.getDeclaredMethod("createVid").setBody(
        TypeFactory.getConstructorCode(vidClassName));
    ctKey.setModifiers(ctKey.getModifiers() & ~Modifier.ABSTRACT);
    CtField[] fields = ctKey.getFields();
    for (CtField f : fields) {
      if (!(f.getDeclaringClass().equals(ctSuper))
          && (f.getModifiers() & Modifier.PRIVATE) == 0)
        ctKey.removeField(f);
    }
    try {
      ctKey.writeFile();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ctKey.toClass();
  }

  public static Class getValueClassByClassName(String vidClassName,
      String vdataClassName, String edataClassName)
      throws CannotCompileException, NotFoundException {
    pool.insertClassPath(new ClassClassPath(IngressJobKeyValueFactory.class));

    // Create a new value class and bind with the new factory.
    CtClass ctVal = pool.get(IngressValueType.class.getName());
    ctVal.setName("generatedclass.MyIngressJobVal" + valid);
    CtClass ctSuper = pool.get(IngressValueType.class.getName());
    ctVal.setSuperclass(ctSuper);
    Class factoryClass = getGraphTypeFactoryByClassName(vidClassName,
        vdataClassName, edataClassName);
    ctVal.getDeclaredMethod("getGraphTypeFactory").setBody(
        "{if (this.factory == null) { this.factory = new "
            + factoryClass.getName() + "(); } return factory;}");
    valid++;

    ctVal.setModifiers(ctVal.getModifiers() & ~Modifier.ABSTRACT);
    CtClass[] inners = ctVal.getNestedClasses();
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

  /**
   * Helper class to create a new {@code GraphTypeFactory} of given types.
   * 
   * @param vidClassName
   * @param vdataClassName
   * @param edataClassName
   * @return
   * @throws NotFoundException
   * @throws CannotCompileException
   */
  private static Class getGraphTypeFactoryByClassName(String vidClassName,
      String vdataClassName, String edataClassName) throws NotFoundException,
      CannotCompileException {
    pool.insertClassPath(new ClassClassPath(GraphTypeFactory.class));
    CtClass ctFactory = pool.get(GraphTypeFactory.class.getName());
    ctFactory.setName("generatedclass.MyGraphTypeFactory" + factoryid);
    factoryid++;
    CtClass ctSuperFactory = pool.get(GraphTypeFactory.class.getName());
    ctFactory.setSuperclass(ctSuperFactory);
    ctFactory.getDeclaredMethod("createVid").setBody(
        TypeFactory.getConstructorCode(vidClassName));
    ctFactory.getDeclaredMethod("createVdata").setBody(
        TypeFactory.getConstructorCode(vdataClassName));
    ctFactory.getDeclaredMethod("createEdata").setBody(
        TypeFactory.getConstructorCode(edataClassName));
    ctFactory.setModifiers(ctFactory.getModifiers() & ~Modifier.ABSTRACT);
    try {
      ctFactory.writeFile();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ctFactory.toClass();
  }

  private static int keyid;
  private static int valid;
  private static int factoryid;
}
