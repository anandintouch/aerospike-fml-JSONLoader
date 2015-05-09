package com.aerospike.customer.fml.jsonloader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.ClientPolicy;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;

/**
 * This class read the formatted JSON file in a stream. Create a Tree model for each Event Node and store 
 * the data in a map and call AerospikeJSONLoader class to create the required Bin with types and load
 *  the data to Aerospike NOSQL DB.
 * 
 * @author anandprakash
 *
 */
public class AerospikeJSONParser {
	
	private static Map<String, Object> truthMap = null;
	private static List<Object> listMap =null;
	
	 private static String HOST;
	 private static int PORT;
	 private static String NAMESPACE ;
	 private static String SET ;
	 private static String filePath;
	 private static AerospikeClient client;
	 private static AerospikeJSONLoader loader;
	 int counter =0;
	 int eventId ;
	 
	 private static Logger logger = Logger.getLogger("AerospikeJSONParser");
	
	/*private static final Map<?, ?>[] RECORDS = {
		truthMap
	};
*/
	public static void main(String[] args) {
		filePath = args[0];
		HOST = args[1];
		PORT = Integer.valueOf(args[2]);
		NAMESPACE = args[3];
		SET = args[4];

		AerospikeJSONParser ajp = new AerospikeJSONParser();
		ajp.createAeroDBConnection(HOST,PORT);
		
		loader = new AerospikeJSONLoader(client,NAMESPACE,SET);
		
		long sT = System.currentTimeMillis();
		System.out.println("Starting at = " + sT);
		logger.info("Starting at = " + sT);
		
		//ajp.parseJSONStream();
		ajp.parseJSONStreamNew();
		
		long eT = System.currentTimeMillis();
		System.out.println("Time taken= " + (eT-sT) + "ms");
		logger.info("Time taken= " + (eT-sT) + "ms");
		//loadDatatoDB();
		
	}
	
	public void parseJSONStreamNew(){

		JsonFactory f = new MappingJsonFactory();
		JsonParser jp;
		
			try {
				jp = f.createJsonParser(new File(filePath));

			    JsonToken current;
	
			    current = jp.nextToken();
			    if (current != JsonToken.START_OBJECT) {
			      System.out.println("Error: root should be object: quiting.");
			      return;
			    }
	
			    while (jp.nextToken() != JsonToken.END_OBJECT) {
		        	
			      String fieldName = jp.getCurrentName();
			      // move from field name to field value
			      current = jp.nextToken();
			      
			      if (fieldName.equals("Events")) {

			        if (current == JsonToken.START_ARRAY) {
			          // For each of the records in the array
			          while (jp.nextToken() != JsonToken.END_ARRAY) {
			            // read the record into a tree model,
			            // this moves the parsing position to the end of it
			            JsonNode node = jp.readValueAsTree();
			          
			            // And now we have random access to everything in the object
			           // System.out.println("Event id : " + node.get("EventID"));
			            if(node.get("EventID")!=null){
			            	
			            	processEachJsonNodeNew(node);
			            }
			            
			          }
			          
			        } else {
			          System.out.println("Error: records should be an Object: skipping.");

			          jp.skipChildren();
			        }
			      } else {
			        System.out.println("Unprocessed property: " + fieldName);
			       // jp.skipChildren();
			      }
   
			    }
			} catch (FileNotFoundException ex) {
				ex.printStackTrace();
			} catch (IOException ex) {
	        	ex.printStackTrace();
	        	logger.error(ex);
	        } catch (NullPointerException ex) {
	        	ex.printStackTrace();
	        } catch (Exception e) {
	        	logger.error(e);
				e.printStackTrace();
			}
	}
	
	public void processEachJsonNodeNew(JsonNode nodeObj) throws Exception{
		listMap = new ArrayList<>();
		truthMap = new HashMap<>();
		if ( nodeObj!= null && !nodeObj.isArray()) {
			
			
			eventId = nodeObj.get("EventID").intValue();
			System.out.println("Event Id: " + eventId);
			
			truthMap.put("EventID", eventId);
			logger.info("The Event id is: "+eventId);
			 
			JsonNode hitData = nodeObj.get("HitData");
			
/*			System.out.println("cell: " + hitData.get("Cell"));
			System.out.println("plane: " + hitData.get("Plane"));
			System.out.println("PeCorr: " + hitData.get("PECorr"));
			System.out.println("PartId: " + hitData.get("PartID"));*/
			
			JsonNode cellArray =  hitData.get("Cell");
              
            List<Integer> cellList = new ArrayList<Integer>();
            for(JsonNode cellObj : cellArray){
            	cellList.add(cellObj.asInt());
          	
            }
            truthMap.put("Cell", cellList);
            
            List<Integer> planeList = new ArrayList<Integer>();
            for(JsonNode planeObj : hitData.get("Plane")){
            	planeList.add(planeObj.asInt());
          	
            }
            truthMap.put("Plane", planeList);
            
            List<Integer> peCorrList = new ArrayList<Integer>();
            for(JsonNode peCorrObj : hitData.get("PECorr")){
            	peCorrList.add(peCorrObj.asInt());
          	
            }
            truthMap.put("PECorr",  peCorrList);
            
            List<Integer> partIdList = new ArrayList<Integer>();
            for(JsonNode partIdObj : hitData.get("PartID")){
            	partIdList.add(partIdObj.asInt());
          	
            }
            truthMap.put("PartID", partIdList);
			
			//Iterator<JsonNode>  eventIteratr = nodeObj.elements();
			loadDatatoDB();
		
		}else{
		    for (final JsonNode objNode : nodeObj) {
		        System.out.println(objNode);
		    }
				
		}
		
	}
	
	public void parseJSONStream(){
		JsonFactory f = new MappingJsonFactory();
		JsonParser jp;
		
			try {
				jp = f.createJsonParser(new File(filePath));

			    JsonToken current;
	
			    current = jp.nextToken();
			    if (current != JsonToken.START_OBJECT) {
			      System.out.println("Error: root should be object: quiting.");
			      return;
			    }
	
			    while (jp.nextToken() != JsonToken.END_OBJECT) {
		        	
			      String fieldName = jp.getCurrentName();
			      // move from field name to field value
			      current = jp.nextToken();
			      if (fieldName.equals("Library")) {
		        
			        if (current == JsonToken.START_ARRAY) {
			          // For each of the records in the array
			          while (jp.nextToken() != JsonToken.END_ARRAY) {
			            // read the record into a tree model,
			            // this moves the parsing position to the end of it
			            JsonNode node = jp.readValueAsTree();
			            
			            // And now we have random access to everything in the object
			           // System.out.println("field1: " + node.get("Event"));
			            if(node.get("Event")!=null){
			            	
			            	processEachJsonNode(node);
			            }
			            
			          }
			          
			        } else {
			          System.out.println("Error: records should be an array: skipping.");
			          jp.skipChildren();
			        }
			      } else {
			        System.out.println("Unprocessed property: " + fieldName);
			        jp.skipChildren();
			        System.out.println("Value is: "+jp.getCurrentName());
			      }
			    }
			} catch (FileNotFoundException ex) {
				ex.printStackTrace();
			} catch (IOException ex) {
	        	ex.printStackTrace();
	        	logger.error(ex);
	        } catch (NullPointerException ex) {
	        	ex.printStackTrace();
	        } catch (Exception e) {
	        	logger.error(e);
				e.printStackTrace();
			}
	}
	
	public void processEachJsonNode(JsonNode nodeObj) throws Exception{
		
		//listMap = new ArrayList<>();
		if ( nodeObj!= null && !nodeObj.isArray()) {
			
			listMap = new ArrayList<>();
			truthMap = new HashMap<>();
			
			Iterator<JsonNode>  eventIteratr = nodeObj.elements();
			while (eventIteratr.hasNext()) {
			    JsonNode eventNode = eventIteratr.next();
			    
			   // int id = eventNode.get("EventID").intValue();
			    eventId = eventNode.get("EventID").intValue();
			    System.out.println("The Event id is: "+eventNode.get("EventID").asInt() );
			    logger.info("The Event id is: "+eventNode.get("EventID").asInt());
			    truthMap.put("EventID",eventId);
			    
			  //  System.out.println("=======Truth object list start======");
                JsonNode truth =  eventNode.get("Truth");


               truthMap.put("Truth.CC", truth.get("CC").asInt());
               truthMap.put("Truth.PDG", truth.get("PDG").asInt());
               // multiply PE by 100
               truthMap.put("Truth.PE", (int)(truth.get("PE").asDouble()*100));
               truthMap.put("Truth.Mode", truth.get("Mode").asInt());
               truthMap.put("Truth.VisE", (int)(truth.get("VisE").asDouble()*100));
               
               //Get the PhotonL arrays elements from the Truth object
               JsonNode photonLArray =  truth.get("PhotonL");
              // System.out.println("The Truth.PhotonL arrays are: ");
               
               List<Integer> photonList = new ArrayList<Integer>();
               for(Object photonLObj : photonLArray){

               	photonList.add((int)Double.parseDouble(photonLObj.toString())*100 );
               	
               }
               truthMap.put("Truth.PhotonL", photonList);
               
               //Get the PhotonE arrays elements from the Truth object
               JsonNode photonEArray =  truth.get("PhotonE");
               //System.out.println("The Truth.PhotonE arrays are: ");
               
               List<Integer> photonEList = new ArrayList<Integer>();
               for(Object photonEObj : photonEArray){
               	photonEList.add((int)Double.parseDouble(photonEObj.toString())*100 );
               	
               }
               truthMap.put("Truth.PhotonE", photonEList);
              
               
               //Get the ParticlesPDG arrays elements from the Truth object
               JsonNode particlesPDGArray =  truth.get("ParticlesPDG");
             //  System.out.println("The Truth.ParticlesPDG arrays are: ");
               
               List<String> particleList = new ArrayList<String>();
               for(Object particlesPDGObj : particlesPDGArray){
               	particleList.add(particlesPDGObj.toString());
               	
               }
               truthMap.put("Truth.PartPDG", particleList);
           //    System.out.println("=======Truth object list end======");
               truthMap.put("NHits", eventNode.get("NHits").asInt());
               //Check for the HitList array from event object
               JsonNode histlistArray =  eventNode.get("HitList");
               
              // System.out.println("\n The Hitlist arrays are: ");
               iterateHitlistElements(histlistArray);

               loadDatatoDB();
			}//end of while
			 
			
		}else{
		    for (final JsonNode objNode : nodeObj) {
		        System.out.println(objNode);
		    }
				
		}
		//loadDatatoDB();
		
	}
	
	private void iterateHitlistElements(JsonNode histlistArray){
		List<Object> hitList = new ArrayList<Object>();
        List<Integer> planesList =  new ArrayList<Integer>();
        List<Integer> cellsList = new ArrayList<Integer>();
        List<Integer> idsList = new ArrayList<Integer>();
        List<Integer> energiesList = new ArrayList<Integer>();
        
        for(JsonNode histlistObj : histlistArray) {
        	planesList.add(histlistObj.get(0).asInt());
        	cellsList.add(histlistObj.get(1).asInt());
        	idsList.add(histlistObj.get(2).asInt());
     	   //String it = histlistObj.get(3).toString();
     	  // double doubleVal =Double.parseDouble(it)*10000;
     	   double doubleVal = histlistObj.get(3).asDouble()*10000;
     	   energiesList.add((int)doubleVal);
   		   
        }
        truthMap.put("Planes", planesList);
        truthMap.put("Cells", cellsList);
        truthMap.put("Ids", idsList);
        truthMap.put("Energies", energiesList);
		
	}
	
	private  void loadDatatoDB() throws AerospikeException{
		//add to the list of map data
		listMap.add(truthMap);
		// Just in case some got left behind
	    // loader.deleteAll(listMap);  // use this if you want to clean up data before loading
		
		//load data
		loader.createRecords(listMap,eventId);

	}
	
    private  void createAeroDBConnection(String host,int port){
		
    	try {
    		ClientPolicy cp = new ClientPolicy();

    		client = new AerospikeClient(cp, host,port);
			
			System.out.println("\n");
			
		} catch (AerospikeException e) {
			e.printStackTrace();
			logger.error(e);
		}
    }
	 

}
