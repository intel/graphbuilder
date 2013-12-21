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
package com.intel.hadoop.graphbuilder.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

/**
 * Represents a function of type: T2 -> T1 -> T2. Provides the interface for getting
 * parameters from Hadoop {@code JobConf}.
 * 
 * @param <T1>
 * @param <T2>
 */

public interface Functional<T1, T2 extends Writable> {

  public class FunctionalConfigurationError extends RuntimeException {
        public FunctionalConfigurationError() {}

        public FunctionalConfigurationError(String message)
        {
            super(message);
        }
  }
  void configure(Configuration configuration) throws FunctionalConfigurationError;

  T2 reduce(T1 a, T2 b);

  Class<T1> getInType();

  Class<T2> getOutType();

  T2 identityValue();
}
