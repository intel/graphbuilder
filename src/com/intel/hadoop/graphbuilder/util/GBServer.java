package com.intel.hadoop.graphbuilder.util;

import java.lang.Character;
import java.io.*;
import java.net.*;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.JSONArray;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;

/**
 * This program is for serving remote request
 * It will be extended to be a multithreaded server
 */
public class GBServer {

    ServerSocket GBServerSocket;
    Socket connection = null;
    ObjectOutputStream out;
    ObjectInputStream in;
    String request;
    HBaseConfiguration conf;
    HashMap hm;

    GBServer(){}

    void run()
    {
        try{
            GBServerSocket = new ServerSocket(2013);
            System.out.println("Waiting for connection");
            connection = GBServerSocket.accept();
            System.out.println("Connection received from " + connection.getInetAddress().getHostName());
            out = new ObjectOutputStream(connection.getOutputStream());
            out.flush();
            in = new ObjectInputStream(connection.getInputStream());
            
            do{
                try{
                    request = (String)in.readObject();
                    if (request.equals("quit"))
                        break;

                    StringTokenizer st = new StringTokenizer(request);
                    String command = (String)st.nextElement();
                    String tableName = (String)st.nextElement();
                    String nodeId = (String)st.nextElement();
                    String direction=null;
                    String level=null;

                    //hard code the request format now..
                    if (command.equals("SubGraph")) {
                        level = (String)st.nextElement();
                    } else if (command.equals("edgelist")) {
                        direction = (String)st.nextElement();
                    } else if (command.equals("vdata")){

                    } 
                    else {
                        System.out.println("does not support this type of command yet..");
                        sendResponse("does not support this type of command yet..");
                    }

                    if (tableName.isEmpty() ||
                        nodeId.isEmpty())
                        sendResponse("Invalid request!!!!");
                    
                    if (!hm.containsKey(tableName)) {
                        HTable htable = new HTable(conf, tableName);            
                        hm.put(tableName, htable);
                    }

                    if (command.equals("SubGraph")) {
                        //hard code 2 level now
                        System.out.println("Receiving subgraph request...");
                        String subgraph = getNewSubGraphinNestedJson(tableName, nodeId, level);
                        sendResponse(subgraph);
                    } else if (command.equals("edgelist")){
                        System.out.println("Receiving edgelist request...");
                        String edgeInfo = getEdgeInfo (tableName, nodeId, direction, true);
                        String edgeInfoInJson = generateEdgeListinJson(edgeInfo, nodeId, direction, 0);
                        sendResponse(edgeInfoInJson);
                    } else if (command.equals("vdata")) {
                        System.out.println("Receiving vertex data request...");
                        String vdata = getVertexData ( tableName, nodeId);
                        sendResponse(vdata);
                    } 
                    else {
                    
                    }
                }
                catch(Exception e){
                    e.printStackTrace();
                }
           } while(!request.equals("quit"));
        }
        catch(IOException e){
            e.printStackTrace();
        }
        finally{
            try{
                in.close();
                out.close();
                GBServerSocket.close();
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }
    }


   void sendResponse(String response)
    {
        try{
            out.writeObject(response);
            out.flush();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    void init() throws Exception {
        conf = new HBaseConfiguration();
        hm = new HashMap(); 
    }

    void close() throws Exception {
        Set set = hm.entrySet(); 
        Iterator i = set.iterator();
        
        while(i.hasNext()) {
            Map.Entry me = (Map.Entry)i.next();
            HTable table = (HTable)me.getValue();
            table.close();
        }
        hm.clear();
    } 

    boolean isNumberic(String str) 
    {
        for (char c : str.toCharArray())
        {
            if (!Character.isDigit(c)) return false;
        }
        return true;
    } 

    boolean isReadable(String input)
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
 
    String getNewSubGraph ( String tableName, String nodeId, String level) throws Exception
    {

        int nodeLevel = Integer.parseInt(level);
        String inEdgeInfoInJson;
        String outEdgeInfoInJson;

        int numEdgestoDisplay = 20;

        JSONObject obj = new JSONObject();
        StringWriter out = new StringWriter();

        String inEdgeInfo = getEdgeInfo(tableName, nodeId, "in", false);
        inEdgeInfoInJson = generateEdgeListinJson(inEdgeInfo, nodeId, "in", numEdgestoDisplay);
        
        String outEdgeInfo = getEdgeInfo(tableName, nodeId, "out", false);
        outEdgeInfoInJson = generateEdgeListinJson(outEdgeInfo, nodeId, "out", numEdgestoDisplay);

        obj.put("level", 1);
        obj.put("node", nodeId);
        obj.put("inEdges", inEdgeInfoInJson);
        obj.put("outEdges", outEdgeInfoInJson);
        obj.writeJSONString(out);
        out.append("\n");

        ArrayList<String> inEdgeList = generateEdgeListinArray(inEdgeInfo, "nodes", numEdgestoDisplay);
        ArrayList<String> outEdgeList = generateEdgeListinArray(outEdgeInfo, "nodes", numEdgestoDisplay);

        ArrayList<String> edges = new ArrayList<String>();
        if(inEdgeList!=null)
            edges.addAll(inEdgeList);
        if(outEdgeList!=null)
            edges.addAll(outEdgeList);

        for (int i=2; i<=nodeLevel; i++) {

                int size;
                if (edges.size()>10) 
                    size = (int)(edges.size() * 0.2);
                else 
                    size = edges.size();
 
                ArrayList<String> subEdges = new ArrayList<String>();

                for (int j=0; j<size; j++) {
                    String startNode = edges.get(j);
                    String temp = getEdgeInfo(tableName, startNode, "in", false);
                    String temp1 = generateEdgeListinJson(temp, startNode, "in", numEdgestoDisplay);
                    if (temp!=null)
                        subEdges.addAll(generateEdgeListinArray(temp, "nodes", numEdgestoDisplay));
                    String temp2 = getEdgeInfo(tableName, startNode, "out", false);
                    String temp3 = generateEdgeListinJson(temp2, startNode, "out", numEdgestoDisplay);
                    if(temp2!=null)
                        subEdges.addAll(generateEdgeListinArray(temp2, "nodes", numEdgestoDisplay));

                    obj.clear();
                    obj.put("level", i);
                    obj.put("node", startNode);
                    obj.put("inEdges", temp1);
                    obj.put("outEdges", temp3);
                    obj.writeJSONString(out);
                    out.append("\n");
                    
                }
                edges = subEdges;
        }  
        
        return out.toString();
    } 

 
    String getSubGraph(String tableName, String nodeId, String level) throws Exception
    {

        int nodeLevel = Integer.parseInt(level);
        String inEdgeInfoInJson;
        String outEdgeInfoInJson;

        JSONObject obj = new JSONObject();
        StringWriter out = new StringWriter();
        
        String inEdgeInfo = getEdgeInfo(tableName, nodeId, "in", false);
        inEdgeInfoInJson = generateEdgeListinJson(inEdgeInfo, nodeId, "in", 0);

        String outEdgeInfo = getEdgeInfo(tableName, nodeId, "out", false);
        outEdgeInfoInJson = generateEdgeListinJson(outEdgeInfo, nodeId, "out", 0);

        // 2 level is good enough in GB?
        obj.put("level", 1);
        obj.put("startNode", nodeId);
        obj.put("inEdgesInfo", inEdgeInfoInJson);
        obj.put("outEdgesInfo", outEdgeInfoInJson);
        obj.writeJSONString(out);
        out.append("\n");

        ArrayList<String> inEdgeList = generateEdgeListinArray(inEdgeInfo, "nodes", 0);
        ArrayList<String> outEdgeList = generateEdgeListinArray(outEdgeInfo, "nodes", 0);

        if (inEdgeInfoInJson!=null)  {
            int size=0;
            if(inEdgeList.size()>10) 
                size = (int)(inEdgeList.size()*0.1);
            else
                size = inEdgeList.size();

            for (int j=1; j<size; j++) {
                    String startNode = inEdgeList.get(j);
                    String temp = getEdgeInfo(tableName, startNode, "in", false);
                    String temp1 = generateEdgeListinJson(temp, startNode, "in", 0);
                    String temp2 = getEdgeInfo(tableName, startNode, "out", false);
                    String temp3 = generateEdgeListinJson(temp2, startNode, "out", 0);

                    obj.put("level", 2);
                    obj.put("startNode", startNode);
                    obj.put("inEdgesInfo", temp1);
                    obj.put("outEdgesInfo", temp3);
                    obj.writeJSONString(out);
                    out.append("\n");
            }
        } 
        if (outEdgeInfoInJson!=null) {
                
            int size=0;
            if(outEdgeList.size()>10) 
                    size = (int)(outEdgeList.size() * 0.1);
            else
                    size = outEdgeList.size();
            for (int j=1; j<size; j++) {
                    String startNode = outEdgeList.get(j);
                    
                    String temp = getEdgeInfo(tableName, startNode, "in", false);
                    String temp1 = generateEdgeListinJson(temp, startNode, "in", 0);
                    String temp2 = getEdgeInfo(tableName, startNode, "out", false);
                    String temp3 = generateEdgeListinJson(temp2, startNode, "out", 0);

                    obj.put("level", 2);
                    obj.put("startNode", startNode);
                    obj.put("inEdgesInfo", temp1);
                    obj.put("outEdgesInfo", temp3);
                    obj.writeJSONString(out);
                    out.append("\n");
            }
        } 
                    
        return out.toString();
    }

    String getVertexData(String tableName, String nodeId) throws Exception
    {
        byte[] value=null;

        HTable htable = (HTable)hm.get(tableName);

        Get get = new Get(Bytes.toBytes(nodeId));
        Result rs = htable.get(get);

        value = rs.getValue(Bytes.toBytes("vdata"), Bytes.toBytes("vdata"));

        String vdata;
        if(value==null)
            vdata = null;
        else
            vdata =  Bytes.toString(value);

        JSONObject obj = new JSONObject();
        obj.put("vertex", nodeId) ;
        obj.put("vdata", vdata) ;      
        return obj.toString();

    }  
    String getEdgeInfo(String tableName, String nodeId, String inOut, boolean all) throws Exception
    {

        boolean direction=false;
        byte[] value=null;
        byte[] pvalue=null;

        HTable htable = (HTable)hm.get(tableName);

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

    String generateEdgeListinJson(String edgelist, String nodeId, String inOut, int numEdgestoDisplay) throws Exception 
    {

       if (edgelist==null)
            return null;

       ArrayList<String> targetList = new ArrayList<String>();
       ArrayList<String> edataList = new ArrayList<String>();
       // to generate JSON string for edgelist

       int i=0;
       StringTokenizer st = new StringTokenizer(edgelist, "|");
       while (st.hasMoreElements() && (numEdgestoDisplay ==0 || i < numEdgestoDisplay)) {
          String substring = (String)st.nextElement();
          StringTokenizer subst = new StringTokenizer(substring);
          String targetId = (String)subst.nextElement();
          String edata = (String)subst.nextElement();
          targetList.add(targetId);
          edataList.add(edata);
          i++;
       }

       JSONObject obj = new JSONObject();
      /* obj.put("node", nodeId);
       obj.put("remoteNodes", targetList);
       obj.put("edata", edataList);
       obj.put("count", targetList.size());
       obj.put("direction", inOut);

       obj.put("name", nodeId);
       obj.put("children", targetList);
       return obj.toString();*/

        ArrayList<JSONObject> edgesinJsonObject = new ArrayList<JSONObject>();
        for (int k=0; k<targetList.size(); k++) {

                String remoteId = (String)targetList.get(k);
                if (remoteId.length() < 4)
                    continue;
                if (isNumberic(remoteId))
                    continue;
                if (!isReadable(remoteId))
                    continue;
            JSONObject temp1 = new JSONObject();
            temp1.put("name", targetList.get(k));
            edgesinJsonObject.add(temp1);
        }
        obj.put("name", nodeId);
        obj.put("children", edgesinJsonObject);
        return obj.toString();
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

   public static void  main(String[] args) throws Exception {
        
        GBServer server = new  GBServer();
        server.init();
        while(true){
            server.run();
            server.close();
        }
    }

}

