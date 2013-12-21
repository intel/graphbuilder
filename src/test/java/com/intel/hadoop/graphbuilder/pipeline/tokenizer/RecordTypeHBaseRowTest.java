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

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

public class RecordTypeHBaseRowTest {

    @Test
    public void testConstructorGet() {
        ImmutableBytesWritable row     = new ImmutableBytesWritable();
        Result                 columns = new Result();
        RecordTypeHBaseRow     record  = new RecordTypeHBaseRow(row, columns);

        assertNotNull(record);
        assertSame(row, record.getRow());
        assertSame(columns, record.getColumns());
    }

    @Test
    public void testSetGetRow() {
        ImmutableBytesWritable row1     = new ImmutableBytesWritable();
        Result                 columns  = new Result();
        ImmutableBytesWritable row2     = new ImmutableBytesWritable();
        ImmutableBytesWritable row3     = new ImmutableBytesWritable();

        RecordTypeHBaseRow     record  = new RecordTypeHBaseRow(row1, columns);

        assertSame(row1, record.getRow());
        assertSame(columns, record.getColumns());

        record.setRow(row2);
        assertSame(row2, record.getRow());
        assertSame(columns, record.getColumns());

        record.setRow(row3);
        assertSame(row3, record.getRow());
        assertSame(columns, record.getColumns());

    }

    @Test
    public void testSetGetColumns() {
        ImmutableBytesWritable row      = new ImmutableBytesWritable();
        Result                 columns1 = new Result();
        Result                 columns2 = new Result();
        Result                 columns3 = new Result();

        RecordTypeHBaseRow     record  = new RecordTypeHBaseRow(row, columns1);

        assertSame(row, record.getRow());
        assertSame(columns1, record.getColumns());

        record.setColumns(columns2);
        assertSame(row, record.getRow());
        assertSame(columns2, record.getColumns());

        record.setColumns(columns3);
        assertSame(row, record.getRow());
        assertSame(columns3, record.getColumns());
    }
}
