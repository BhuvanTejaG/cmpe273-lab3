package edu.sjsu.cmpe.cache.client.ConsistentHashing;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;


import com.google.common.hash.*;
import edu.sjsu.cmpe.cache.client.CacheService.CacheServiceInterface;
import edu.sjsu.cmpe.cache.client.CacheService.DistributedCacheService;
import edu.sjsu.cmpe.cache.client.ConsistentHashing.ConsistentHash;

public class ConsistentHashingClient {
	
    static SortedMap<Integer, String> nodes =new TreeMap<Integer, String>();
    static HashFunction hf = Hashing.murmur3_128();
    
    private static String server1="http://localhost:3000";
    private static  String server2="http://localhost:3001";
    private static String server3="http://localhost:3002";
   

    static char values[]={'a','b','c','d','e','f','g','h','i','j'};
    
    private static final Funnel<CharSequence> strFunnel = Funnels.stringFunnel(Charset.defaultCharset());
    private static final Funnel<Integer> intFunnel = Funnels.integerFunnel();
    public static void main(String[] args) throws Exception {
 

        List<String> servers = new ArrayList<String>();
        servers.add(server1);
        servers.add(server2);
        servers.add(server3);

        for(int i=0;i<10;i++){
            ConsistentHash<Integer, String> h = new ConsistentHash(Hashing.murmur3_128(),intFunnel,strFunnel,servers);

            String distserver=h.get(new Integer(i));
             
            CacheServiceInterface cacheServer = new DistributedCacheService(distserver);
            System.out.println("placing content in the server:::::"+distserver);
            cacheServer.put(i + 1, String.valueOf(values[i]));
            System.out.println("getting content from the server:::::"+distserver);
            int j = i+1;
            System.out.println(j+" ==> "+cacheServer.get(i+1));
  
        }

    }



}