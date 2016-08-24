package org.apache.tinkerpop.gremlin.tinkergraph.Util;

/**
 * Created by wangzhenzhong on 16/8/5.
 */
public class Utility {
    public static final char DOT = '.';
    public static final char COMMA = '.';
    public static final char ADD_SIGN = '+';
    public static final char MINUS_SIGN = '-';
    public static final char LEFT = '<';
    public static final char RIGHT = '>';

    public static final long SCORE = 0;

    public static final String MSG = "【MSG】";
    public static final String WARNING = "【WARNING】";
    public static final String DATA = "【DATA】";
    public static final String DEBUG = "【DEBUG】";

    public static final String ALLVERTEX = "AllVertex";
    public static final String VERTEX_EDGE = "VE";
    public static final String EDGE_PROPERTIES = "EP";
    public static final String VERTEX_PROPERTIES = "VP";
    public static final String EDGE_LABEL="EL";
    public static final String VERTEX_LABEL="VL";
    public static final String IN_VERTEX_ID="IVID";
    public static final String OUT_VERTEX_ID="OVID";
    public static final String HUGEZSETLAMP = "HLamp";
    public static final String ALLLAMP = "AllLamp";
    public static final String COUNTER = "counter";
    public static final String MIN = "min";
    public static final char PRE_SINGLE = 'S';
    public static final char PRE_MULTI = 'M';
    public static final char TAIL='~';

    public static final boolean showInfoSwitch = true;
    public static final boolean redisSwitch = true;
    public static boolean debugInfoSwitch = false;

    public static long timeFlag() {
        return System.currentTimeMillis();
    }

    public static void programLamp() {
        if (debugInfoSwitch) {
            Throwable throwable = new Throwable();
            String methodName = throwable.getStackTrace()[1].getMethodName();
            String className = throwable.getStackTrace()[1].getClassName();
            int lineNumber = throwable.getStackTrace()[1].getLineNumber();
            System.out.printf(DEBUG + "%s:%s:%d\n", className, methodName, lineNumber);
        }
    }
}
