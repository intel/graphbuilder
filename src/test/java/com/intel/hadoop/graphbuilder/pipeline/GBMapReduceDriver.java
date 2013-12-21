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
package com.intel.hadoop.graphbuilder.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mrunit.internal.mapreduce.ContextDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * MrUnit MapReduceDriver was extended and modified lightly to allow instantiation  with a reduceDriver and mapDriver
 * instead if the Map and Reduce classes. Using drivers allows us to get a context mock before we run the map reduce
 * pipeline.
 *
 * @see MapReduceDriver
 *
 * @param <K1> map input key
 * @param <V1> map input value
 * @param <K2> map output key / reduce input key
 * @param <V2> map output value / reduce input value
 * @param <K3> reduce output key
 * @param <V3> reduce output value
 */
public class GBMapReduceDriver<K1, V1, K2, V2, K3, V3> extends
        MapReduceDriver<K1, V1, K2, V2, K3, V3> implements ContextDriver {

    public static final Log LOG = LogFactory.getLog(GBMapReduceDriver.class);

    private MapDriver<K1, V1, K2, V2> myMapDriver;
    private ReduceDriver<K2, V2, K3, V3> myReduceDriver;

    @SuppressWarnings("rawtypes")
    private Class<? extends OutputFormat> outputFormatClass;
    @SuppressWarnings("rawtypes")
    private Class<? extends InputFormat> inputFormatClass;

    GBMapReduceDriver(MapDriver<K1, V1, K2, V2> mapper, ReduceDriver<K2, V2, K3, V3> reducer){
        super.setCounters(new Counters());
        setMyMapDriver(mapper);
        setMyReduceDriver(reducer);
    }

    public static <K1, V1, K2, V2, K3, V3> MapReduceDriver<K1, V1, K2, V2, K3, V3> newMapReduceDriver(
            final MapDriver<K1, V1, K2, V2> mapper, final ReduceDriver<K2, V2, K3, V3> reducer) {
        return new GBMapReduceDriver<K1, V1, K2, V2, K3, V3>(mapper, reducer);
    }

    /**
     * runs the mapreduce driver. this is the MapReduceDriver's run method modified to use given map drivers not create
     * ones
     *
     * @return key, value pairs that were written to context.write
     * @throws IOException
     */
    @Override
    public List<Pair<K3, V3>> run() throws IOException {
        MapDriver<K1, V1, K2, V2> myMapDriver = getMyMapDriver();
        ReduceDriver<K2, V2, K3, V3> myReduceDriver = getMyReduceDriver();
        try {
            preRunChecks(myMapDriver, getMyReduceDriver());
            initDistributedCache();
            List<Pair<K2, V2>> mapOutputs = new ArrayList<Pair<K2, V2>>();
            // run map component
            LOG.debug("Starting map phase with mapper: " + myMapDriver);
            //this was creating a new map driver i changed it to use the one i give it
            mapOutputs.addAll(myMapDriver
                    .withCounters(getCounters()).withConfiguration(getConfiguration())
                    .withAll(inputList).withMapInputPath(getMapInputPath()).run());

            // Run the reduce phase.
            LOG.debug("Starting reduce phase with reducer: " + myReduceDriver);
            return new ReducePhaseRunner<K3, V3>().runReduce(shuffle(mapOutputs),
                    myReduceDriver);
        } finally {
            cleanupDistributedCache();
        }
    }

    /**
     * The private class to manage starting the reduce phase is used for type
     * genericity reasons. This class is used in the run() method.
     */
    private class ReducePhaseRunner<OUTKEY, OUTVAL> {
        private List<Pair<OUTKEY, OUTVAL>> runReduce(
                final List<Pair<K2, List<V2>>> inputs,
                final ReduceDriver<K2, V2, OUTKEY, OUTVAL> reduceDriver1) throws IOException {

            final List<Pair<OUTKEY, OUTVAL>> reduceOutputs = new ArrayList<Pair<OUTKEY, OUTVAL>>();

            if (!inputs.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    final StringBuilder sb = new StringBuilder();
                    for (Pair<K2, List<V2>> input : inputs) {
                        formatValueList(input.getSecond(), sb);
                        LOG.debug("Reducing input (" + input.getFirst() + ", " + sb + ")");
                        sb.delete(0, sb.length());
                    }
                }

                /*final ReduceDriver<K2, V2, OUTKEY, OUTVAL> myReduceDriver = ReduceDriver*/
                reduceDriver1
                        .withCounters(getCounters())
                        .withConfiguration(getConfiguration()).withAll(inputs);

                if (getOutputSerializationConfiguration() != null) {
                    reduceDriver1
                            .withOutputSerializationConfiguration(getOutputSerializationConfiguration());
                }

                if (outputFormatClass != null) {
                    reduceDriver1.withOutputFormat(outputFormatClass, inputFormatClass);
                }

                reduceOutputs.addAll(reduceDriver1.run());
            }

            return reduceOutputs;
        }
    }

    public MapDriver<K1, V1, K2, V2> getMyMapDriver() {
        return myMapDriver;
    }

    public ReduceDriver<K2, V2, K3, V3> getMyReduceDriver() {
        return myReduceDriver;
    }

    public void setMyMapDriver(MapDriver<K1, V1, K2, V2> myMapDriver) {
        this.myMapDriver = myMapDriver;
    }

    public void setMyReduceDriver(ReduceDriver<K2, V2, K3, V3> myReduceDriver) {
        this.myReduceDriver = myReduceDriver;
    }
}
