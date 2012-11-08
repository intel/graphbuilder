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
package com.intel.hadoop.graphbuilder.test.experimental.javassist;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.Modifier;
import javassist.NotFoundException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressJobKeyValueFactory;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressKeyType;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressValueType;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.EdgeTransformJobValueFactory;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.PairListType;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.PreprocessJobValueFactory;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.VertexEdgeUnionType;
import com.intel.hadoop.graphbuilder.types.EmptyType;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.types.TypeFactory;
import com.intel.hadoop.graphbuilder.util.FsUtil;

public class RuntimeKeyValueTest {

  @Test
  public void modifySelfTest() throws NotFoundException,
      CannotCompileException, InstantiationException, IllegalAccessException {
    ClassPool pool = ClassPool.getDefault();
    CtClass ctfoo = pool
        .get("com.intel.hadoop.graphbuilder.test.experimental.javassist.Foo");
    CtMethod m = ctfoo.getDeclaredMethod("createElem");
    m.setBody("return \"Hello World!\";");
    ctfoo.setModifiers(ctfoo.getModifiers() & ~Modifier.ABSTRACT);
    Class cfoo = ctfoo.toClass();
    Foo obj = ((Foo) (cfoo.newInstance()));
    String path = ctfoo.getURL().getPath();
    obj.run();
  }

  @Test
  public void subClassTest() throws NotFoundException, CannotCompileException,
      InstantiationException, IllegalAccessException {
    ClassPool pool = ClassPool.getDefault();
    CtClass ctbar = pool.get(Foo.class.getName());
    if (ctbar.isFrozen())
      ctbar.defrost();
    ctbar.setName(Foo.class.getName() + "bar");
    ctbar.setSuperclass(pool.get(Foo.class.getName()));
    CtMethod m = ctbar.getMethod("createElem", "()Ljava/lang/Object;");
    m.setBody("return \"Bar\";");
    ctbar.setModifiers(ctbar.getModifiers() & ~Modifier.ABSTRACT);
    Class cbar = ctbar.toClass();
    Foo obj = ((Foo) (cbar.newInstance()));
    obj.run();
  }

  @Test
  public void ingressKeyValFactoryTest() throws ClassNotFoundException {
    try {
      Class keyclass = IngressJobKeyValueFactory
          .getKeyClassByClassName(TypeFactory.getClassName("string"));
      Class valclass = IngressJobKeyValueFactory.getValueClassByClassName(
          TypeFactory.getClassName("string"), TypeFactory.getClassName("none"),
          TypeFactory.getClassName("int"));
      IngressKeyType key = (IngressKeyType) keyclass.newInstance();
      IngressValueType val = (IngressValueType) valclass.newInstance();
      assertEquals(key.vid().getClass(), StringType.class);
      assertEquals(val.getGraphTypeFactory().createVid().getClass(),
          StringType.class);
      assertEquals(val.getGraphTypeFactory().createVdata().getClass(),
          EmptyType.class);
      assertEquals(val.getGraphTypeFactory().createEdata().getClass(),
          IntType.class);
    } catch (NotFoundException e) {
      e.printStackTrace();
    } catch (CannotCompileException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void preproessKeyValFactoryTest() {
    try {
      Class valclass = PreprocessJobValueFactory.getValueClassByClassName(
          TypeFactory.getClassName("string"), TypeFactory.getClassName("none"),
          TypeFactory.getClassName("int"));
      VertexEdgeUnionType val = (VertexEdgeUnionType) valclass.newInstance();
      assertEquals(val.createVid().getClass(), StringType.class);
      assertEquals(val.createVdata().getClass(), EmptyType.class);
      assertEquals(val.createEdata().getClass(), IntType.class);
    } catch (NotFoundException e) {
      e.printStackTrace();
    } catch (CannotCompileException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void edgeTransformKeyValFactoryTest() {
    try {
      Class stringintclass = EdgeTransformJobValueFactory
          .getValueClassByClassName(TypeFactory.getClassName("string"),
              TypeFactory.getClassName("int"));
      PairListType val1 = (PairListType) stringintclass.newInstance();
      assertEquals(val1.createLValue().getClass(), StringType.class);
      assertEquals(val1.createRValue().getClass(), IntType.class);

      val1.init(new StringType("hello"), new IntType(1));
      val1.add(new StringType("world"), new IntType(2));
      int i = val1.size();
      assertEquals(val1.size(), 2);
      PairListType val0 = (PairListType) stringintclass.newInstance();
      assertEquals(val0.size(), 0);
      val0.add(new StringType("foo"), new IntType(3));
      assertEquals(val0.size(), 1);
      val0.append(val1);
      assertEquals(val0.size(), val1.size() + 1);

      Class intstringclass = EdgeTransformJobValueFactory
          .getValueClassByClassName(TypeFactory.getClassName("int"),
              TypeFactory.getClassName("string"));
      PairListType val2 = (PairListType) intstringclass.newInstance();
      assertEquals(val2.createLValue().getClass(), IntType.class);
      assertEquals(val2.createRValue().getClass(), StringType.class);

      assertEquals(val1.size(), 2);
    } catch (NotFoundException e) {
      e.printStackTrace();
    } catch (CannotCompileException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}
