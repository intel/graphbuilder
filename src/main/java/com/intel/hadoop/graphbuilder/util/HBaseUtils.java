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

import com.intel.hadoop.graphbuilder.pipeline.input.hbase.GBHTableConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * The utility library for graphbuilder's hbase operations.
 *
 * Implemented as a singleton.
 */
public class HBaseUtils {

    private static HBaseUtils uniqueInstanceOfHBaseUtils = null;

    private static final Logger LOG = Logger.getLogger(HBaseUtils.class);

    private Configuration configuration;
    private HBaseAdmin    admin;
    private String        hTableName;
    private HTable        hTable;

    /*
     * PRIVATE constructor method... this is a singleton class, remember?
     *
     * @throws IOException
     */
    private HBaseUtils() throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.admin         = new HBaseAdmin(configuration);
    }

    /**
     * Private constructor method.
     *
     * @throws IOException
     */
    private HBaseUtils(String tableName) throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.hTableName    = tableName;
        this.hTable        = new HTable(configuration, hTableName);
    }

    /**
     *  Returns the unique instance of {@code HBaseUtils}; creates one if there isn't one already.
     *
     *  @throws IOException
     *
     */

    public static synchronized HBaseUtils getInstance() throws IOException {

        if (uniqueInstanceOfHBaseUtils == null) {
                uniqueInstanceOfHBaseUtils = new HBaseUtils();
        }

        return uniqueInstanceOfHBaseUtils;
    }

    /**
     * Returns a new hbase configuration.
     */
    public static Configuration getNewConfiguration() {
        return HBaseConfiguration.create();
    }

    /**
     * Parses the column name to return the family and qualifier in a string array.
     *
     * @param {@code columnName}  The column name in HBase "column family:column qualifier".
     * @return The family and qualifier in string array.
     */
    public static byte[][] parseColumnName(String columnName) {
        return KeyValue.parseColumn(Bytes.toBytes(columnName));
    }

    /**
     * Gets the cell value from HBase given a column handler and column name.
     *
     * @param {@code columns}         The scanned columns from an HTable row passed from a mapper or reducer.
     * @param {@Code fullColumnName}  The full column key, for example: "family:qualifier".
     * @return The cell value as a byte array.
     */
    public static byte[] getColumnData(Result columns, String fullColumnName) {
        byte[][] columnKey = HBaseUtils.parseColumnName(fullColumnName);

        if (columnKey.length < 2) {
            return null;
        }

        // Read cell from HTable

        return columns.getValue(columnKey[0], columnKey[1]);
    }

    /**
     * Gets the column value from HBase given a table handler, rowkey, and column key.
     *
     * @param {@code Key}            The row key.
     * @param {@code colFamilyName}  The column family name.
     * @param {@code colName}        The column name.
     * @return The column value as a byte array.
     */

    public byte[] getColumnData(String Key, String colFamilyName, String colName)
            throws IOException, NumberFormatException {

        byte[] rowKey     = Bytes.toBytes(Key);
        Get    getRowData = new Get(rowKey);
        Result res        = hTable.get(getRowData);

        return res.getValue(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName));
    }

    /**
     * Writes a cell value to HBase.
     *
     * @param {@code key}              The HBase row key.
     * @param {@code columnFamily}     The HBase column family (default - "VertexID").
     * @param {@code columnQualifier}  The HBase column name.
     * @param {@code value}            The value to write.
     * @param {@code context}          The context of Hadoop's reducer.
     * @return value          The value that was written.
     */
    public static byte[] putValue(byte[] key,
                                  byte[] columnFamily,
                                  byte[] columnQualifier,
                                  byte[] value,
                                  Reducer.Context context) throws IOException, InterruptedException {
        Put put = new Put(key);
        put.add(columnFamily, columnQualifier, value);
        context.write(new Text(GBHTableConfiguration.config.getProperty("NULLKEY")), put);
        return value;
    }

    /**
     * Checks if the table exists in HBase.
     *
     * @param {@code hTableName}  The HBase table name.
     * @return true If, and only if, the table with the given name exists.
     */
    public boolean tableExists(String hTableName) throws IOException {
        return admin.tableExists(hTableName);
    }

    /**
     * If the table exists in HBase, it is disabled and dropped.
     */
    public void removeTable(String testTableName) throws IOException {

        if (admin.tableExists(testTableName)) {
            admin.disableTable(testTableName);
            if (admin.isTableDisabled(testTableName)) {
                admin.deleteTable(testTableName);
            } else {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.HBASE_ERROR,
                        "GRAPHBUILDER ERROR: Unable to delete existing table " + testTableName + ". Please delete it",
                        LOG);
            }
        }
    }

    /**
     * Checks if the given table contains the given column family.
     *
     * @param hTableName The HBase table name.
     * @param columnFamilyName  The family name of the column.
     * @return true If, and only if, the table contains the given column family.
     */
    public boolean tableContainsColumnFamily(String hTableName, String columnFamilyName) throws IOException {
        HTableDescriptor htd = admin.getTableDescriptor(hTableName.getBytes());
        return htd.hasFamily(columnFamilyName.getBytes());
    }

    /**
     * Checks if the given full column has a column family that is present in the table.
     *
     * @param {@code fullColumnName}  The full column name.
     * @param {@code tableName}       The table name.
     * @return  true If, and only if, the column's family is present in the table.
     */
    public boolean columnHasValidFamily( String fullColumnName, String tableName) {

        boolean returnValue = false;

        if (fullColumnName.contains(":")) {
            String columnFamilyName = fullColumnName.split(":")[0];
            try {
                returnValue = tableContainsColumnFamily(tableName, columnFamilyName);
            } catch (IOException e) {
                GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                        "GRAPHBUILDER FAILURE: unhandled IO exception while validating column family with hbase.", LOG, e);
            }
        }

        return returnValue;
    }

    /**
     * Returns the configuration of the {@code HBaseUtils} instance.
     * @return The configuration of the {@code HBaseUtils} instance.
     */
    public Configuration getConfiguration() {
        return this.configuration;
    }

    /**
     * Creates an HBase table.
     *
     * @param {@code hTableName}            The name of the HBase table to be created.
     * @param {@code hTableColumnFamilies}  The names of the table column families.
     * @return A scan for the table.
     */
    public Scan createTable(String hTableName, String[] hTableColumnFamilies) throws IOException {

        Scan scan = getTableScanner(hTableName);

        // Delete the vertex ID temporary table and create it again for this job

        if (admin.tableExists(this.hTableName)) {

            admin.disableTable(this.hTableName);

            if (admin.isTableDisabled(this.hTableName)) {
                admin.deleteTable(this.hTableName);
            } else {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.HBASE_ERROR,
                        "GRAPHBUILDER ERROR: Unable to delete existing table " + this.hTableName + ". Please delete it",
                        LOG);
            }
        }

        HTableDescriptor htd = new HTableDescriptor(this.hTableName);

        for (String colDesc : hTableColumnFamilies) {
            HColumnDescriptor hcd = new HColumnDescriptor(colDesc);
            htd.addFamily(hcd);
        }

        admin.createTable(htd);
        LOG.info("GRAPHBUILDER_INFO: " + this.hTableName + " table created");

        if (!admin.tableExists(this.hTableName)) {
            throw new IOException("GRAPHBUILDER_ERROR: Failed to create table " + this.hTableName);
        }

        return scan;
    }

    /**
     * Creates an HBase table with a single column family.
     *
     * @param {@code hTableName}          The name of the HBase table to be created.
     * @param {@code hTableColumnFamily}  The names of the table column family.
     * @return A scan for the table.
     */
    public Scan createTable(String hTableName, String hTableColumnFamily) throws IOException {

        Scan scan = getTableScanner(hTableName);

        // Deletes the vertex ID temporary table and creates it again for this job.

        if (admin.tableExists(this.hTableName)) {

            admin.disableTable(this.hTableName);

            if (admin.isTableDisabled(this.hTableName)) {
                admin.deleteTable(this.hTableName);
            } else {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.HBASE_ERROR,
                        "GRAPHBUILDER ERROR: Unable to delete existing table " + this.hTableName + ". Please delete it",
                        LOG);
            }
        }

        HTableDescriptor  htd = new HTableDescriptor(this.hTableName);
        HColumnDescriptor hcd = new HColumnDescriptor(hTableColumnFamily);

        htd.addFamily(hcd);

        admin.createTable(htd);

        LOG.info("GRAPHBUILDER_INFO: " + this.hTableName + " table created");

        if (!admin.tableExists(this.hTableName)) {
            throw new IOException("GRAPHBUILDER ERROR: Failed to create table " + this.hTableName);
        }

        return scan;
    }

    /**
     * Gets a scanner for the specified table.
     * @param {@code tableName}   The name of the table in question.
     * @return  A scanner for the specified table.
     */
    public Scan getTableScanner(String tableName) {
        this.hTableName = tableName;
        Scan scan       = new Scan();

        scan.setCaching(HBaseConfig.config.getPropertyInt("HBASE_CACHE_SIZE"));
        scan.setCacheBlocks(false);

        return scan;
    }
}
