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
package com.intel.hadoop.graphbuilder.util;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Parallel for equivalent implementation.
 * 
 * @author Haijie Gu
 * 
 */
public class Parallel {
  private static final int NUM_CORES = Runtime.getRuntime()
      .availableProcessors();
  private static final int NUM_PROCS = NUM_CORES;

  /**
   * Funtion interface for the parfor operation.
   * 
   * @author Haijie Gu
   * 
   * @param <T>
   */
  public static interface Operation<T> {
    public void perform(T pParameter, int idx);
  }

  public Parallel() {
    forPool = Executors.newFixedThreadPool(NUM_PROCS);
  }

  public <T> void For(final List<T> pElements, final Operation<T> pOperation) {
    ExecutorService executor = forPool;
    List<Future<?>> futures = new LinkedList<Future<?>>();

    final int batchsize = pElements.size() < NUM_PROCS ? 1 : 1 + ((pElements
        .size() - 1) / NUM_PROCS);
    for (int i = 0; i < NUM_PROCS; ++i) {
      final int pid = i;
      Future<?> future = executor.submit(new Runnable() {
        @Override
        public void run() {
          final int start = batchsize * pid;
          final int end = Math.min(batchsize * (pid + 1), pElements.size());
          for (int idx = start; idx < end; ++idx)
            pOperation.perform(pElements.get(idx), idx);
        }
      });
      futures.add(future);
    }

    for (Future<?> f : futures) {
      try {
        f.get();
      } catch (InterruptedException e) {
      } catch (ExecutionException e) {
      }
    }
  }

  /**
   * Reap the threads.
   */
  public void close() {
    forPool.shutdown();
    while (!forPool.isTerminated()) {
    }
  }

  private ExecutorService forPool;
}
