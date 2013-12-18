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
 * This program is to gete in/out edge list of a nodeid
 */
public class GetEdgeList {

    public static boolean isNumberic(String str)
    {
        for (char c : str.toCharArray())
        {
            if (!Character.isDigit(c)) return false;
        }
        return true;
    }

    public static boolean isReadable(String input)
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

    public static void  main(String[] args) throws Exception {

        if (args.length !=3 ) {
            System.out.println("GetEdgesList needs three parameters");
            System.out.println("GetEdgesList: table nodeId in/out/all");
            System.exit(0);
        }

        int direction=0;
        byte[] value=null;
        byte[] value1=null;
        byte[] pvalue=null;
        byte[] pvalue1=null;
        String tableName = args[0];
        String nodeId = args[1];
        if (args[2].equals("in"))
            direction = 0;
        else if (args[2].equals("out"))
            direction = 1;
        else
            direction = 2;

        HBaseConfiguration conf = new HBaseConfiguration();
        HTable htable = new HTable(conf, tableName);

        Get get = new Get(Bytes.toBytes(nodeId));
        
        Result rs = htable.get(get);

        String edgelist="";
        if (direction == 0) {
            value = rs.getValue(Bytes.toBytes("inEdges"), Bytes.toBytes("inEdges"));
            if (value==null)
                return;
            edgelist = Bytes.toString(value);
        }
        else if (direction==1){
            value = rs.getValue(Bytes.toBytes("outEdges"), Bytes.toBytes("outEdges"));
            if (value==null)
                return;
            edgelist = Bytes.toString(value);

        } else {
            value = rs.getValue(Bytes.toBytes("outEdges"), Bytes.toBytes("outEdges"));
            if (value!=null) {
                edgelist = Bytes.toString(value);
            } 
            value1 = rs.getValue(Bytes.toBytes("inEdges"), Bytes.toBytes("inEdges"));
            if (value1!=null) {
                edgelist += Bytes.toString(value1);
            }
            if (value==null && value1==null)
                return;
        }


        int i=1;

        // should add qualifer in hbase to indicate whether it has more rows..       
        while(true) {
          Get pget = new Get(Bytes.toBytes(nodeId+"-p"+Integer.toString(i)));
          i++;
          Result prs = htable.get(pget);
          if (direction == 0) {
             pvalue = prs.getValue(Bytes.toBytes("inEdges"), Bytes.toBytes("inEdges"));
             if(pvalue==null)
                 break;
            else {
                edgelist += Bytes.toString(pvalue);
            }

          }
          else if (direction==1){
              pvalue = prs.getValue(Bytes.toBytes("outEdges"), Bytes.toBytes("outEdges"));
             if(pvalue==null)
                break;
             else {
               edgelist += Bytes.toString(pvalue);
             }
          } else {
             pvalue = prs.getValue(Bytes.toBytes("outEdges"), Bytes.toBytes("outEdges"));
             if(pvalue!=null)
                edgelist += Bytes.toString(pvalue);
             pvalue1 = prs.getValue(Bytes.toBytes("inEdges"), Bytes.toBytes("inEdges"));
            if(pvalue1!=null)
                edgelist += Bytes.toString(pvalue1);
            if (pvalue==null && pvalue1==null)
                break;
         }
        } 

        ArrayList<String> targetList = new ArrayList<String>();
        ArrayList<String> edataList = new ArrayList<String>();

        StringTokenizer st = new StringTokenizer(edgelist, "|");
        while (st.hasMoreElements()) {
          String substring = (String)st.nextElement();
          StringTokenizer subst = new StringTokenizer(substring);
          String targetId = (String)subst.nextElement();
          String edata = (String)subst.nextElement();
          targetList.add(targetId);
          edataList.add(edata);
        }
        JSONObject obj = new JSONObject();
/*        obj.put("node", nodeId);
        obj.put("remoteNodes", targetList);
        obj.put("edata", edataList);
        obj.put("count", targetList.size());
        obj.put("direction", direction);
       
        obj.put("name", nodeId);
        obj.put("children", targetList);
*/
        ArrayList<JSONObject> edgesinJsonObject = new ArrayList<JSONObject>();

        int threshold =15;
        for (int k=0; k<targetList.size(); k++) {
            String remoteId = (String)targetList.get(k);
            if (remoteId.length() < 4)
                continue;
            if (GetEdgeList.isNumberic(remoteId))
                continue;
            if (!GetEdgeList.isReadable(remoteId))
                continue;
            if (k>threshold)
                break;
            JSONObject temp1 = new JSONObject();
            temp1.put("name", targetList.get(k));
            edgesinJsonObject.add(temp1);
        }
        obj.put("name", nodeId);
        obj.put("children", edgesinJsonObject);
       
        System.out.println(obj.toString());

        htable.close();
    }
}

