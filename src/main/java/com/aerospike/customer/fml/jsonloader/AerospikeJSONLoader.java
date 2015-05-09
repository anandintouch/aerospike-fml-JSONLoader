package com.aerospike.customer.fml.jsonloader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.node.DoubleNode;

public class AerospikeJSONLoader {
	
    private static String TEST_NAMESPACE = "test";
    private static String TEST_SET = "fermilabset";
    
    private AerospikeClient client;

    @SuppressWarnings("static-access")
	public AerospikeJSONLoader(AerospikeClient aInClient,String namespace,String set)
    {
        client = aInClient;
        this.TEST_NAMESPACE = namespace;
        this.TEST_SET = set;
    }
    
	
    public void createRecords(Map<?, ?>[] dataMap) throws AerospikeException
    {
    	int counter =0;
    	
        WritePolicy lPolicy = new WritePolicy();
        lPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        lPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        lPolicy.generation = 0;
        for (Map<?, ?> lRecord : dataMap)
        {
            @SuppressWarnings("unchecked")
            Bin[] lBins = getBins((Map<String, Object>) lRecord);
            //Key lKey = getKey(lRecord);
            Key lKey = new Key(TEST_NAMESPACE, TEST_SET, Value.get(counter));
            client.put(lPolicy, lKey, lBins);
            counter++;
        }
    }
    
    public void createRecords(List<Object> listMap,int counter) throws AerospikeException
    {
        WritePolicy lPolicy = new WritePolicy();
       // lPolicy.timeout = 50;  
       // lPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
       // lPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
       // lPolicy.generation = 0;
        for(Object obj : listMap){

        	for (Object lRecord : Arrays.asList(obj))
            {
                @SuppressWarnings("unchecked")
                Bin[] lBins = getBins((Map<String, Object>) lRecord);

                Key lKey = new Key(TEST_NAMESPACE, TEST_SET, Value.get(counter));
                client.put(lPolicy, lKey, lBins);
               
            }
        	
        }
        
    }
	
    
	private static Bin[] getBins(Map<String, Object> aInValues)
    {
    	StringBuilder sb  = new StringBuilder();
    	
        List<Bin> lBins = new ArrayList<>();
        for (Map.Entry<String, Object> lEntry : aInValues.entrySet())
        {
            Object lValue = lEntry.getValue();
            Bin lBin;
            if (lValue instanceof List)
            {
                lBin = Bin.asList(lEntry.getKey(), (List<?>) lValue);
                
            }
            else if (lValue instanceof Map)
            {
                lBin = Bin.asMap(lEntry.getKey(), (Map<?, ?>) lValue);
            }
            else
            {
            	//int intVal = (int) lValue;
            	//intVal =  (int) lValue;
            	lBin = new Bin(lEntry.getKey(), lValue);
            
            }
            
            lBins.add(lBin);
        }
       // int val = sb.length()-1;
        //sb.deleteCharAt(val);
        
       // System.out.println("aggregated value is: "+sb.toString());
       // lBins.add(new Bin("AggregatedBin",sb.toString()));
        
        // Convert list to an array and return it
        return lBins.toArray(new Bin[lBins.size()]);
    }
    
    private static Key getKey(Map<?, ?> aInRecord) throws AerospikeException
    {
        return new Key(TEST_NAMESPACE, TEST_SET, Value.get(aInRecord.get("EventID")));
    }
    
    @SafeVarargs
    public static <T> List<T> list(T... aInValues)
    {
        return Arrays.asList(aInValues);
    }
    
    public static Map<?, ?> map(
            Object... aInKeysAndValues)
    {
        Map<Object, Object> lMap = new HashMap<>();
        for (int i = 0; i < aInKeysAndValues.length - 1; i += 2)
        {
            lMap.put(aInKeysAndValues[i], aInKeysAndValues[i + 1]);
        }
        return lMap;
    }

    
    public void deleteAll(List<Object> listMap) throws AerospikeException
    {
    	int counter =0;
    	
        WritePolicy lPolicy = new WritePolicy();
        lPolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
       // lPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
       // lPolicy.generation = 1;
        for(Object obj : listMap){
        	for (Object lRecord : Arrays.asList(obj))
            {
            	Key lKey = new Key(TEST_NAMESPACE, TEST_SET, Value.get(counter));
                client.delete(lPolicy, lKey);
                counter ++;
            }
        }
        
    }

}
