package com.intel.hadoop.graphbuilder.util;

import java.util.StringTokenizer;
import java.util.ArrayList;

import net.minidev.json.JSONObject;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;

/**
 * This program is to get vdata of a nodeid
 */
public class GetVertexData {

    public static void  main(String[] args) throws Exception {

        if (args.length !=2 ) {
            System.out.println("GetVertexData needs two parameters");
            System.out.println("GetVertexData: table nodeId");
            System.exit(0);
        }

        byte[] value=null;
        String tableName = args[0];
        String nodeId = args[1];

        HBaseConfiguration conf = new HBaseConfiguration();
        HTable htable = new HTable(conf, tableName);

        Get get = new Get(Bytes.toBytes(nodeId));
        
        Result rs = htable.get(get);

        value = rs.getValue(Bytes.toBytes("vdata"), Bytes.toBytes("vdata"));
        if (value==null)
          System.out.println("there is no vertex: "+ nodeId);
  
        String vdata = Bytes.toString(value);
        System.out.println("nodeId: " + nodeId + " data is: " + vdata);
        htable.close();

    }
}

