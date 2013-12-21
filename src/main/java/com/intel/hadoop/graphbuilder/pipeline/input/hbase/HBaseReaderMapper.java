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
package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.RecordTypeHBaseRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

/**
 * Reads columns from an {@code HTable} and emits vertices and edges. Most of the  
 * logic has been moved to a {@code BaseMapper} class that is also used by 
 * {@code TextParsingMapper}.
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper
 */
public class HBaseReaderMapper extends TableMapper<IntWritable, SerializedGraphElement> {
    private static final Logger LOG = Logger.getLogger(HBaseReaderMapper.class);

    private BaseMapper baseMapper;

    /**
     * Most of the code has been removed from {@code setup} and moved to the  
     * {@code BaseMapper} class. This makes everything less complex, including exception handling. 
	 * Any errors during setup will be caught by the {@code BaseMapper} class and logged as fatal 
	 * and a {@code system.exit} will be called.
     *
     * @param {@code context} The mapper context.
     */

    @Override
    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();

        //Initializes the tokenizer key function and map key and map values.

        setBaseMapper(new BaseMapper(context, conf, LOG));
    }

    /**
     * Maps the input of HTable rows and columns to vertices and edges. Any exception thrown by 
     * {@code contex.write} inside the base mapper class will be caught and logged as errors so we can
     * continue to the next record.	 
     *
     * @param {@code row}      The row key.
     * @param {@code columns}  The columns of the row.
     * @param {@code context}  The task context.
     */

    @Override
    public void map(ImmutableBytesWritable row, Result columns, Context context) {

        context.getCounter(GBHTableConfiguration.Counters.HTABLE_ROWS_READ).increment(1);

        RecordTypeHBaseRow record = getRecordTypeHBaseRow(row, columns);

        baseMapper.getTokenizer().parse(record, context);

        //The base mapper class handles writing edges and vertices as well as exception handling.

        baseMapper.writeEdges(context);

        baseMapper.writeVertices(context);
    }

    public void setBaseMapper(BaseMapper baseMapper) {
        this.baseMapper = baseMapper;
    }

    private RecordTypeHBaseRow getRecordTypeHBaseRow(ImmutableBytesWritable row, Result columns) {
        return new RecordTypeHBaseRow(row, columns);
    }
}
