package com.orientechnologies.orient.stresstest.util;

public class OConstants {

    public final static String OPTION_MODE = "m";
    public final static String OPTION_ITERATIONS = "n";
    public final static String OPTION_THREADS = "t";
    public final static String OPTION_OPERATIONS = "s";
    public final static String OPTION_ROOT_PASSWORD = "p";
    public final static String OPTION_REMOTE_IP = "remote-ip";
    public final static String AVAILABLE_OPTIONS = OPTION_MODE + OPTION_ITERATIONS + OPTION_THREADS + OPTION_OPERATIONS + OPTION_ROOT_PASSWORD;

    public static final String SYNTAX = "OStressTester \n\t-m [plocal|memory|remote|distributed] \n\t-n iterationsNumber \n\t-s operationSet \n\t-t threadsNumber \n\t-p rootPassword (optional)"; //\t--remote-ip 192.168.1.1\n";
    public static final String TEMP_DATABASE_NAME = "stress-test-db-";
    public static final String CLASS_NAME = "StressTestDoc";
    public static final String INDEX_NAME = CLASS_NAME + ".Index";

    public static final String VERSION = "0.1";
}
