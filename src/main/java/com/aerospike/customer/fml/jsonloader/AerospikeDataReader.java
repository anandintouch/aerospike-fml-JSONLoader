package com.aerospike.customer.fml.jsonloader;

import java.util.Arrays;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

/**
 * Data retrieval class.
 *
 */
public class AerospikeDataReader 
{
	private static AerospikeClient client = null;
	
    final static String host = "localhost";
	final static int port = 3000;
	
    private static final String TEST_NAMESPACE = "fermi_ns1";
    private static final String TEST_SET = "fermilabset";
	
    public void queryRecordUsingSecondaryIndex(WritePolicy policy,String search){
    	RecordSet rs = null;
		try {
			createSecondaryIndex(policy);
			//Create statement 
	    	Statement stmt = new Statement();
	    	stmt.setNamespace(TEST_NAMESPACE);
	    	stmt.setSetName(TEST_SET); 
	    	stmt.setFilters( Filter.equal("AggregatedBin", search) ); 
	    	stmt.setBinNames("AggregatedBin"); // optional
	    	
	    	//Execute query
	    	rs = client.query(null, stmt);
	    	
	    	
	    	//Iterate through the RecordSet
	    	int count = 0;
	    	while (rs.next()) {
	    		count++;
	            Key key = rs.getKey();
	            Record record = rs.getRecord();
	            
	            System.out.println("----Fetch using 'Query' by providing Secondary Index----");
	            System.out.println( "Record "+count+" is: "+record.bins.toString() );
	            
	            
	        }
	    	if( count == 0 ){
	    		System.out.println("Record not found!");
	    	}
	    	
	    	
		} catch (AerospikeException e) {
			e.printStackTrace();
		}finally{
			rs.close();
		}

    	
    }
    
    private void queryUsingGetCall(){
    /*	Key[] keys = new Key[2];
    	//keys[0] = new Key(TEST_NAMESPACE, TEST_SET, 0);
		keys[1] = new Key(TEST_NAMESPACE, TEST_SET, 1);
    	Record[] recordset = client.get(null, keys);*/
    	
    	Record record = client.get(null, new Key(TEST_NAMESPACE, TEST_SET, 0));
    	System.out.println( "\nData from DB: "+ record.toString() );
    	
    	/*
    	for(Record rec : Arrays.asList(recordset)){
			//Object obj = rec.bins;
			System.out.println( "\nData from DB: "+rec.toString() );
	    	//System.out.println( "\nData from DB: "+obj.toString() );
	    	for (Map.Entry<String, Object> lEntry : ((Map<String, Object>)obj).entrySet()){
	    		System.out.println("Record is: Key= "+lEntry.getKey()+ " Value is= "+lEntry.getValue());
	    		if( lEntry.getKey().equals("Plane")){
	    			System.out.println("Plane is- "+lEntry.getValue());
	    			
	    			
	    		}
	    	}
		}*/
    }
	
    private void createAeroDBConnection(String host,int port){
		
    	try {
			client = new AerospikeClient(host, port);

			
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
    }
    
    /*
     * Create Secondary Index on bin.Secondary indexes need to be created once on the server
     */
    private static void createSecondaryIndex(WritePolicy policy) throws AerospikeException{
    	IndexTask task = client.createIndex(policy, "test", "fermilabset", 
    		    "idx_test_fermilabset_age", "AggregatedBin", IndexType.STRING);
    	task.waitTillComplete();
    }
    
	public static void main( String[] args ) throws Exception
    {
		AerospikeDataReader adr = new AerospikeDataReader();
		adr.createAeroDBConnection(host,port);
		
		// Initialize policy.
		WritePolicy policy = new WritePolicy();
		policy.timeout = 15;
		policy.sendKey= true;
		
		/*createSecondaryIndex(policy);
		
		String searchStr = "1-0.0-0.0-EA-14-1-260-0.0-0.0-0";
		adr.queryRecordUsingSecondaryIndex(policy,searchStr);*/
		
		long sT = System.currentTimeMillis();
		adr.queryUsingGetCall();
		long eT = System.currentTimeMillis();
		System.out.println("Time taken= " + (eT-sT) + "ms");
		
		client.close();
		
    }
	
}
