package org.apache.tinkerpop.gremlin.tinkergraph.Util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import static org.apache.tinkerpop.gremlin.tinkergraph.Util.Utility.showInfoSwitch;

/**
 * Created by wangzhenzhong on 16/8/5.
 */
public class RedisResource {

        public static String IP = "";
        public static int PORT = 0;
        public static int BUFFERSIZE = 0;
        public static long SETSKIPSIZE = 0;
        
        private static JedisPoolConfig jfc = null;
        private static JedisPool jpool = null;
        public static Jedis jedis = null;
        
        /*
        {
        	System.out.println("===start redis resource config...");
        	BufferedReader in = null;
            String str = null;
            String[] temp = null;
            try {
                in = new BufferedReader(new FileReader(System.getProperty("user.dir") + "/redis.properties"));
            
                while ((str = in.readLine()) != null) {
                    temp = str.split("=");
                    if (temp[0].equals("RedisServerIP"))
                        IP = temp[1];
                    if (temp[0].equals("RedisServerPort"))
                        PORT = Integer.parseInt(temp[1]);
                    if (temp[0].equals("BufferSize"))
                    	BUFFERSIZE = Integer.parseInt(temp[1]);
                    if (temp[0].equals("SetSkipSize"))
                    	SETSKIPSIZE = Integer.parseInt(temp[1]);
                    
                    System.out.println("---redis config: "+str);
                }
                
                jfc = new JedisPoolConfig();
                jpool = new JedisPool(jfc, IP, PORT);
                if (showInfoSwitch) System.out.println(Utility.MSG + "Connect to "+IP+" DB.");
                jedis = jpool.getResource();
                
            } catch (Exception e) {
                e.printStackTrace();
            } 
        }
		*/
        
        public static void setRedisConfig(String ip, String port, String bfsize, String skipsize)
        {
        	IP = ip;
            PORT = Integer.parseInt(port);
            BUFFERSIZE = Integer.parseInt(bfsize);
            SETSKIPSIZE = Integer.parseInt(skipsize);
            
            jfc = new JedisPoolConfig();
            jpool = new JedisPool(jfc, IP, PORT);
            if (showInfoSwitch) System.out.println(Utility.MSG + "Connect to "+IP+" DB.");
            jedis = jpool.getResource();
        }
}
