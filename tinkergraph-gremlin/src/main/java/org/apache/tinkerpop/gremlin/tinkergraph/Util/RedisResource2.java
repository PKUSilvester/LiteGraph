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
public class RedisResource2 {

        public static String IP = getIP();
        public static int PORT = getPORT();
        public static int BUFFERSIZE = getBufferSize();
        public static long SETSKIPSIZE = getSetSkipSize();
        public static Information information = getInformation();

        private static JedisPoolConfig jfc;
        private static JedisPool jpool;
        public static Jedis jedis = getJedis();


        public static class Information {
            String ip;
            int port;
            int buffersize;
            int setSkipSize;
            Jedis jedis;
        }

        private static Information getInformation() {

            Information information = new Information();
            BufferedReader in = null;
            String str = null;
            String[] temp = null;
            try {
                in = new BufferedReader(new FileReader(System.getProperty("user.dir") + "/redis.properties"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            try {
                while ((str = in.readLine()) != null) {
                    temp = str.split("=");
                    if (temp[0].equals("RedisServerIP"))
                        information.ip = temp[1];
                    if (temp[0].equals("RedisServerPort"))
                        information.port = Integer.parseInt(temp[1]);
                    if (temp[0].equals("BufferSize"))
                        information.buffersize = Integer.parseInt(temp[1]);
                    if (temp[0].equals("SetSkipSize"))
                        information.setSkipSize = Integer.parseInt(temp[1]);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return information;
        }


        private static int getBufferSize() {
            return getInformation().buffersize;
        }

        private static int getSetSkipSize() {
            return getInformation().setSkipSize;
        }

        private static Jedis getJedis() {
            jfc = new JedisPoolConfig();
            jpool = new JedisPool(jfc, getInformation().ip, getInformation().port);
            if (showInfoSwitch) System.out.println(Utility.MSG + "Connect to "+getInformation().ip+" DB.");
            return jpool.getResource();
        }

        private static String getIP() {
            return getInformation().ip;
        }

        private static int getPORT() {
            return getInformation().port;
        }



}
