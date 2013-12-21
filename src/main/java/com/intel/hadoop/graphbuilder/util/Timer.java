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

/**
 * A simple timer class that can be used for benchmarking or timing up to
 * microsecond resolution.
 * 
 */
public class Timer {
  private long start;
  private long current;

  /**
   * Initializes the timer with the current time.
   */
  public Timer() {
    start = System.currentTimeMillis();
  }

  /**
   * Resets the timer with current time.
   */
  public void start() {
    current = start = System.currentTimeMillis();
  }

  /**
   * @return The time in seconds passed since last reset.
   */
  public long current_time() {
    current = System.currentTimeMillis();
    return (current - start) / 1000;
  }

  /**
   * @return The time in milliseconds passed since last reset.
   */
  public long current_time_millis() {
    current = System.currentTimeMillis();
    return current - start;
  }

  /**
   * @return The time passed since last read.
   */
  public long time_since_last() {
    long ret = (System.currentTimeMillis() - current) / 1000;
    current = System.currentTimeMillis();
    return ret;
  }

  /**
   * @return The time in milliseconds passed since last read.
   */
  public long time_since_last_millis() {
    long ret = (System.currentTimeMillis() - current);
    current = System.currentTimeMillis();
    return ret;
  }

}
