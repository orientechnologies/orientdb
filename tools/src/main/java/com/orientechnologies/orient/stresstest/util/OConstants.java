/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.stresstest.util;

/**
 * Some constants used by the StressTester
 *
 * @author Andrea Iacono
 */
public class OConstants {

    public final static String OPTION_MODE = "m";
    public final static String OPTION_ITERATIONS = "n";
    public final static String OPTION_THREADS = "t";
    public final static String OPTION_TRANSACTIONS = "x";
    public final static String OPTION_OPERATIONS = "s";
    public final static String OPTION_ROOT_PASSWORD = "root-password";
    public final static String OPTION_REMOTE_IP = "remote-ip";
    public final static String OPTION_REMOTE_PORT = "remote-port";
    public final static String MAIN_OPTIONS = OPTION_MODE + OPTION_ITERATIONS + OPTION_THREADS + OPTION_OPERATIONS + OPTION_TRANSACTIONS;

    public static final String SYNTAX = "StressTester " +
            "\n\t-m mode (can be any of these: [plocal|memory|remote|distributed] )" +
            "\n\t-n iterationsNumber " +
            "\n\t-s operationSet" +
            "\n\t-t threadsNumber" +
            "\n\t-x operationsPerTransaction" +
            "\n\t--root-password rootPassword" +
            "\n\t--remote-ip ipOrHostname" +
            "\n\t--remote-port portNumber" +
            "\n";
    public static final String TEMP_DATABASE_NAME = "stress-test-db-";
    public static final String CLASS_NAME = "StressTestDoc";
    public static final String INDEX_NAME = CLASS_NAME + ".Index";

    public static final String VERSION = "0.1";
    public static final String CONSOLE_REMOTE_PASSWORD_PROMPT = "OrientDB Server (%s:%d) - Please insert the root password to create the test database: ";
}
