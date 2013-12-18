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
package com.intel.hadoop.graphbuilder.test.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.intel.hadoop.graphbuilder.util.Parallel;
import com.intel.hadoop.graphbuilder.util.Parallel.Operation;

/**
 * Test for {@code Parallel}.
 * 
 * @see Parallel
 * 
 */
public class ParallelTest {

  public class MutableInt {
    public MutableInt(int val) {
      this.val = val;
    }

    public MutableInt(MutableInt other) {
      this.val = other.val;
    }

    public int val;
  }

  /**
   * Test ParFor a[i] = a[i] + i.
   */
  @Test
  public void testIndexOp() {
    ArrayList<MutableInt> list = new ArrayList<MutableInt>();
    Random rnd = new Random();
    int maxiter = 150;
    for (int i = 0; i < maxiter; ++i)
      list.add(new MutableInt(rnd.nextInt()));

    ArrayList<MutableInt> copylist = new ArrayList<MutableInt>();
    for (int i = 0; i < maxiter; ++i)
      copylist.add(new MutableInt(list.get(i).val));
    Parallel parfor = new Parallel();
    parfor.For(list, new Operation<MutableInt>() {
      @Override
      public void perform(MutableInt pParameter, int idx) {
        pParameter.val += idx;
      }

    });
    parfor.close();

    for (int i = 0; i < maxiter; ++i) {
      int j = list.get(i).val;
      int k = copylist.get(i).val + i;
      assertEquals(j, k);
    }
  }

  /**
   * Test counting: ParFor counter[a[i]] = a.filter(x == a[i]).size()
   */
  @Test
  public void testSort() {
    int maxiter = 10;
    int maxint = 999;
    ArrayList<Integer> toSort = new ArrayList<Integer>(maxiter);
    Random rnd = new Random();
    for (int i = 0; i < maxiter; ++i)
      toSort.add(rnd.nextInt(maxint));

    final ArrayList<AtomicInteger> counterArray = new ArrayList<AtomicInteger>(
        maxint);
    for (int i = 0; i < maxint; ++i)
      counterArray.add(new AtomicInteger(0));

    final ArrayList<AtomicInteger> counterArray2 = new ArrayList<AtomicInteger>(
        maxint);
    for (int i = 0; i < maxint; ++i)
      counterArray2.add(new AtomicInteger(0));

    Parallel parfor = new Parallel();
    parfor.For(toSort, new Operation<Integer>() {
      @Override
      public void perform(Integer p, int idx) {
        counterArray.get(p).incrementAndGet();
      }
    });
    parfor.close();

    for (int i = 0; i < toSort.size(); ++i) {
      counterArray2.get(toSort.get(i)).incrementAndGet();
    }

    for (int i = 0; i < counterArray.size(); ++i)
      assertEquals(counterArray.get(i).get(), counterArray2.get(i).get());
  }
}
