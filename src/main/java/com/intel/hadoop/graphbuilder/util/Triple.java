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

import java.util.Objects;

/**
 * Represents a triple of objects.
 *
 *
 * @param {@code <A>}
 * @param {@code <B>}
 * @param {@code <C>}
 */
public class Triple<A, B, C> {

    /**
     * Constructs a triple.
     *
     * @param {@code a}
     * @param {@code b}
     * @param {@code c}
     */
    public Triple(A a, B b, C c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    /**
     * @return The A value.
     */
    public A getA() {
        return this.a;
    }

    /**
     * @return The B value.
     */
    public B getB() {
        return this.b;
    }

    /**
     * @return The C value.
     */
    public C getC() {
        return this.c;
    }

    /**
     * @param {@code val}   The new value for the A value.
     */
    public void setA(A val) {
        this.a = val;
    }

    /**
     * @param {@code val}   The new value for the B value.
     */
    public void setB(B val) {
        this.b = val;
    }

    /**
     * @param {@code val}   The new value for the C value.
     */
    public void setC(C val) {
        this.c = val;
    }

    /**
     * @return Reverse the first two coordinates.
     */
    public Triple<B, A, C> swapAB() {
        return new Triple<B, A, C>(b, a, c);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Triple) {
            Triple t = (Triple) obj;
            return Objects.equals(t.a,a)
                   && Objects.equals(t.b,b)
                   && Objects.equals(t.c,c);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return HashUtil.hashTriple(a, b, c);
    }

    @Override
    public String toString() {
        return "(" + a + ", " + b + ", " + c + ")";
    }

    private A a;
    private B b;
    private C c;
}

