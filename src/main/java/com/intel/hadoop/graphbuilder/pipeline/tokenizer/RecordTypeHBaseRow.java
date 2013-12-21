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
package com.intel.hadoop.graphbuilder.pipeline.tokenizer;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Encapsulates hbase reader output as one data object.
 *
 * It has two parts:
 * <ul>
 *     <li>{@code ImmutableBytesWritable row }  The hbase row.</li>
 *     <li>{@code Result columns} The columns of the row.</li>
 * </ul>
 * @see com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper
 */
public class RecordTypeHBaseRow {

    private ImmutableBytesWritable row;
    private Result                 columns;

    /**
     * A Constructor that takes a row and its columns.
     * @param {@code row}      The {@code ImmutableBytesWriteable} row.
     * @param {@code columns}  The {@code Results columns} of the row.
     */
    public RecordTypeHBaseRow(ImmutableBytesWritable row, Result columns) {
        this.row     = row;
        this.columns = columns;
    }

    public void setRow(ImmutableBytesWritable row) {
        this.row = row;
    }

    public ImmutableBytesWritable getRow() {
        return this.row;
    }

    public void setColumns(Result columns) {
        this.columns = columns;
    }

    public Result getColumns() {
        return this.columns;
    }
}
