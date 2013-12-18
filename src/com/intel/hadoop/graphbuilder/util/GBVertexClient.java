package com.intel.hadoop.graphbuilder.util;

import java.io.*;
import java.net.*;

/** 
 * A simple GB client example
 */
public class GBVertexClient{

    Socket clientSocket;
    ObjectOutputStream out;
    ObjectInputStream in;
    GBVertexClient(){}
    
    void run()
    {
        try{
            clientSocket = new Socket("localhost", 2013);
            out = new ObjectOutputStream(clientSocket.getOutputStream());
            out.flush();
            in = new ObjectInputStream(clientSocket.getInputStream());
            
            try{
                String tableName = "vTable";
                String nodeId = "0A-10_Warthog"; //one docid
                //hard code the request format: tablename nodeid direction
                String message = "vdata" + "\t" + tableName + "\t" + 
                                nodeId;

                sendRequest(message);
                String response = (String)in.readObject();
                processResponse(response);
                sendRequest("quit");
            }  catch(ClassNotFoundException classNot){
                System.err.println("data received in unknown format");
            }
        }
        catch(UnknownHostException e){
            System.err.println("You are trying to connect to an unknown host!");
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
        finally{
            try{
                in.close();
                out.close();
                clientSocket.close();
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }
    }
    void processResponse(String response) 
    {
        //return Json format
        System.out.println("response is: " + response);
    } 

    void sendRequest(String msg)
    {
        try{
            out.writeObject(msg);
            out.flush();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String args[])
    {
        GBVertexClient client = new GBVertexClient();
        client.run();
    }
}
