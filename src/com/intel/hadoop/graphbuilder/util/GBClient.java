package com.intel.hadoop.graphbuilder.util;

import java.io.*;
import java.net.*;

/** 
 * A simple GB client example
 */
public class GBClient{

    Socket clientSocket;
    ObjectOutputStream out;
    ObjectInputStream in;
    GBClient(){}
    
    void run()
    {
        try{
            clientSocket = new Socket("localhost", 2013);
            out = new ObjectOutputStream(clientSocket.getOutputStream());
            out.flush();
            in = new ObjectInputStream(clientSocket.getInputStream());
            
            try{
                String tableName = "adjTable";
                String nodeId = "0A-10_Warthog"; //one docid
                String direction ="out" ; // or "in"
                //hard code the request format: tablename nodeid direction
                String message = "edgelist" + "\t" + tableName + "\t" + 
                                nodeId + "\t"  + direction;
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
        GBClient client = new GBClient();
        client.run();
    }
}
