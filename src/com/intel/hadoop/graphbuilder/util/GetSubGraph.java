package com.intel.hadoop.graphbuilder.util;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.io.PrintWriter;

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
 * This program is to gete in/out edge list of a nodeid
 */
public class GetSubGraph {

   public boolean isNumberic(String str)
    {
        for (char c : str.toCharArray())
        {
            if (!Character.isDigit(c)) return false;
        }
        return true;
    }

    public boolean isReadable(String input)
    {
        boolean isASCII = true;
        for (int i = 0; i < input.length(); i++) {
            int c = input.charAt(i);
            if (c > 0x7F) {
                isASCII = false;
                break;
            }
        }
        return isASCII;
    }

    String getEdgeInfo(String tableName, String nodeId, String inOut, boolean all) throws Exception
    {

        boolean direction=false;
        byte[] value=null;
        byte[] pvalue=null;

        if (inOut.equals("in"))
            direction = false;
        else
            direction = true;

        Get get = new Get(Bytes.toBytes(nodeId));

        Result rs = htable.get(get);

        if (direction == false)
            value = rs.getValue(Bytes.toBytes("inEdges"), Bytes.toBytes("inEdges"));
        else
            value = rs.getValue(Bytes.toBytes("outEdges"), Bytes.toBytes("outEdges"));

        if (value==null)
            return null;

        String edgelist = Bytes.toString(value);

       // should add one qualifer like more to indicate whther we need to read more rows
        if (all) {
            int i=1;
            while(true) {
                Get pget = new Get(Bytes.toBytes(nodeId+"-p"+Integer.toString(i)));
                i++;
                Result prs = htable.get(pget);
                if (direction == false) {
                pvalue = prs.getValue(Bytes.toBytes("inEdges"), Bytes.toBytes("inEdges"));
                }
                else {
                    pvalue = prs.getValue(Bytes.toBytes("outEdges"), Bytes.toBytes("outEdges"));
                }
                if(pvalue==null)
                   break;
                else {
                  edgelist += Bytes.toString(pvalue);
                }
            }
        }

        return edgelist;
    }
   ArrayList<String> generateEdgeListinArray(String edgelist, String type, int numEdgestoDisplay) throws Exception
   {
       if (edgelist==null)
            return null;

       ArrayList<String> targetList = new ArrayList<String>();
       ArrayList<String> edataList = new ArrayList<String>();
       StringTokenizer st = new StringTokenizer(edgelist, "|");

       int i = 0;

       while (st.hasMoreElements() && (numEdgestoDisplay ==0 || i < numEdgestoDisplay)) {
          String substring = (String)st.nextElement();
          StringTokenizer subst = new StringTokenizer(substring);
          String targetId = (String)subst.nextElement();
          String edata = (String)subst.nextElement();
          targetList.add(targetId);
          edataList.add(edata);
          i++;
       }

       if (type.equals("nodes"))
           return targetList;
       else if (type.equals("edata"))
            return edataList;
       else
            return null;
   }

    JSONObject getChildren(String tableName, String nodeId, String nodeLevel) throws Exception
    {
        int level = Integer.parseInt(nodeLevel);
        if (level==1) {
            String inEdgeInfo = getEdgeInfo(tableName, nodeId, "in", false);
            ArrayList<String> inEdgeList = generateEdgeListinArray(inEdgeInfo, "nodes", 25);
            String outEdgeInfo = getEdgeInfo(tableName, nodeId, "out", false);
            ArrayList<String> outEdgeList = generateEdgeListinArray(outEdgeInfo, "nodes", 25);
            JSONObject obj = new JSONObject();
            ArrayList<String> edges = new ArrayList<String>();
            if(inEdgeList!=null)
               edges.addAll(inEdgeList);
            if(outEdgeList!=null)
                edges.addAll(outEdgeList);

            ArrayList<JSONObject> edgesinJsonObject = new ArrayList<JSONObject>();

            for (int i=0; i<edges.size(); i++) {

                String remoteId = (String)edges.get(i);
                if (remoteId.length() < 4)
                    continue;
                if (isNumberic(remoteId))
                    continue;
                if (!isReadable(remoteId))
                   continue;

                JSONObject temp1 = new JSONObject();
                temp1.put("name", edges.get(i));
                edgesinJsonObject.add(temp1);
            }
            obj.put("name", nodeId);
            //obj.put("children", edges);
            obj.put("children", edgesinJsonObject);

            return obj;
        } else if (level>1) {
            String inEdgeInfo = getEdgeInfo(tableName, nodeId, "in", false);
            ArrayList<String> inEdgeList = generateEdgeListinArray(inEdgeInfo, "nodes", 25);
            String outEdgeInfo = getEdgeInfo(tableName, nodeId, "out", false);
            ArrayList<String> outEdgeList = generateEdgeListinArray(outEdgeInfo, "nodes", 25);
            ArrayList<String> edges = new ArrayList<String>();
            if(inEdgeList!=null)
               edges.addAll(inEdgeList);
            if(outEdgeList!=null)
                edges.addAll(outEdgeList);

            JSONObject obj = new JSONObject();
            ArrayList<JSONObject> children = new ArrayList<JSONObject>();
            for (int i=0; i<edges.size(); i++) {
                String remoteId = (String)edges.get(i);
                if (remoteId.length() < 4)
                    continue;
                if (isNumberic(remoteId))
                    continue;
                if (!isReadable(remoteId))
                   continue;

                JSONObject temp = getChildren(tableName, (String)edges.get(i), Integer.toString(level-1));
                children.add(temp);
          }
            obj.put("name", nodeId);
            obj.put("children", children);
            return obj;
        } else
            return null;
    }

    String getNewSubGraphinNestedJson ( String tableName, String nodeId, String level) throws Exception
   {

        JSONObject obj =getChildren(tableName, nodeId, level);
        return obj.toString();
   }

    void init(String tableName) throws Exception
    {
        conf = new HBaseConfiguration();
        htable = new HTable(conf, tableName);
    }
    void close() throws Exception
    {   
        if (htable !=null)
            htable.close();
    }  
    public static void  main(String[] args) throws Exception {

        if (args.length !=4 ) {
            System.out.println("GetSubGraph needs four parameters");
            System.out.println("GetSubGraph: table nodeId level outputfile");
            System.exit(0);
        }
        String tableName = args[0];
        if (tableName == null)
            tableName = "adjTable";
        String nodeId = args[1];
        if (nodeId == null)
            nodeId = "0A-10_Warthog";
        String level = args[2];
        if (level==null)
            level = "2";

        String output = args[3];
        if (output==null)
            output = "ouput";

        PrintWriter out = new PrintWriter(output);
        GetSubGraph subGraph = new GetSubGraph();
        subGraph.init(tableName);
        String result = subGraph.getNewSubGraphinNestedJson(tableName, nodeId, level);
        subGraph.close();
        
        System.out.println("Subgraph is: " + result);
        out.println(result);
        out.close();
    }

    HBaseConfiguration conf;
    HTable htable;
}

