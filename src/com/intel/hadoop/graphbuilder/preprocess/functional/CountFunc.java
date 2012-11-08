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
package com.intel.hadoop.graphbuilder.preprocess.functional;

import java.lang.reflect.ParameterizedType;

import org.apache.hadoop.mapred.JobConf;

import com.intel.hadoop.graphbuilder.types.IntType;

/**
 * Represents a counting functional: f x y -> y + 1
 * 
 * 
 * @param <T>
 */
public class CountFunc<T> implements Functional<T, IntType> {

  @Override
  public void configure(JobConf job) throws Exception {
  }

  @Override
  public IntType reduce(T a, IntType b) {
    return new IntType(b.get() + 1);
  }

  @Override
  public Class<T> getInType() {
    return (Class) ((ParameterizedType) this.getClass().getGenericSuperclass())
        .getActualTypeArguments()[0];
  }

  @Override
  public Class<IntType> getOutType() {
    return IntType.class;
  }

  @Override
  public IntType base() {
    return IntType.ZERO;
  }
}
