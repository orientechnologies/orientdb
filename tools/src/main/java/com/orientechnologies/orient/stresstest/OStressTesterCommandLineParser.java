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
package com.orientechnologies.orient.stresstest;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.stresstest.workload.OWorkload;

import java.io.Console;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This is the parser of the command line arguments passed with the invocation of OStressTester. It contains a static method that -
 * given the arguments - returns a OStressTester object.
 *
 * @author Andrea Iacono
 */
public class OStressTesterCommandLineParser {
  public static final String TEMP_DATABASE_NAME             = "stress-test-db-";
  public static final String CONSOLE_REMOTE_PASSWORD_PROMPT = "OrientDB Server (%s:%d) - Please insert the root password to create the test database: ";

  public final static String OPTION_CONCURRENCY                    = "c";
  public final static String OPTION_MODE                           = "m";
  public final static String OPTION_WORKLOAD                       = "w";
  public final static String OPTION_TRANSACTIONS                   = "tx";
  public final static String OPTION_DELAY                          = "delay";
  public final static String OPTION_KEEP_DATABASE_AFTER_TEST       = "k";
  public final static String OPTION_OUTPUT_FILE                    = "o";
  public static final String OPTION_PLOCAL_PATH                    = "d";
  public final static String OPTION_LOAD_BALANCING                 = "lb";
  public final static String OPTION_DBNAME                         = "db";
  public final static String OPTION_CHECK_DATABASE                 = "chk";
  public final static String OPTION_ROOT_PASSWORD                  = "root-password";
  public static final String ERROR_OPENING_CONSOLE                 =
      "An error has occurred opening the console. Please supply the root password as the -" + OPTION_ROOT_PASSWORD + " parameter.";
  public final static String OPTION_REMOTE_IP                      = "remote-ip";
  public final static String OPTION_HA_METRICS                     = "ha-metrics";
  public static final String COMMAND_LINE_PARSER_MISSING_REMOTE_IP =
      "The mode is [" + OStressTester.OMode.REMOTE + "] but the param --" + OPTION_REMOTE_IP + " wasn't passed.";
  public final static String OPTION_REMOTE_PORT                    = "remote-port";

  public final static String MAIN_OPTIONS =
      OPTION_MODE + OPTION_CONCURRENCY + OPTION_WORKLOAD + OPTION_TRANSACTIONS + OPTION_DELAY + OPTION_OUTPUT_FILE
          + OPTION_PLOCAL_PATH + OPTION_KEEP_DATABASE_AFTER_TEST + OPTION_CHECK_DATABASE + OPTION_LOAD_BALANCING + OPTION_DBNAME;

  public static final String SYNTAX =
      "StressTester " + "\n\t-m mode (can be any of these: [plocal|memory|remote|distributed] )" + "\n\t-w workloads"
          + "\n\t-c concurrency-level" + "\n\t-x operations-per-transaction" + "\n\t-o result-output-file"
          + "\n\t-d database-directory" + "\n\t-k true|false" + "\n\t-chk true|false" + "\n\t--root-password rootPassword"
          + "\n\t--remote-ip ipOrHostname" + "\n\t--remote-port portNumber" + "\n\t-lb load-balancing-strategy" + "\n\t-db db-name"
          + "\n";

  static final String COMMAND_LINE_PARSER_INVALID_NUMBER                  = "Invalid %s number [%s].";
  static final String COMMAND_LINE_PARSER_LESSER_THAN_ZERO_NUMBER         = "The %s value must be greater than 0.";
  static final String COMMAND_LINE_PARSER_INVALID_MODE                    = "Invalid mode [%s].";
  static final String COMMAND_LINE_PARSER_INVALID_OPTION                  = "Invalid option [%s]";
  static final String COMMAND_LINE_PARSER_EXPECTED_VALUE                  = "Expected value after argument [%s]";
  static final String COMMAND_LINE_PARSER_INVALID_REMOTE_PORT_NUMBER      = "Invalid remote port [%d]. The port number has to be lesser than 65536.";
  static final String COMMAND_LINE_PARSER_MODE_PARAM_MANDATORY            = "The mode param [-m] is mandatory.";
  static final String COMMAND_LINE_PARSER_NOT_EXISTING_OUTPUT_DIRECTORY   = "The directory where to write the resultOutputFile [%s] doesn't exist.";
  static final String COMMAND_LINE_PARSER_NOT_EXISTING_PLOCAL_PATH        = "The plocal directory (param -d) doesn't exist [%s].";
  static final String COMMAND_LINE_PARSER_NO_WRITE_PERMISSION_OUTPUT_FILE = "You don't have the permissions for writing on directory [%s] the resultOutputFile.";
  static final String COMMAND_LINE_PARSER_NO_WRITE_PERMISSION_PLOCAL_PATH = "You don't have the permissions for writing on plocal directory [%s].";
  static final String COMMAND_LINE_PARSER_PLOCAL_PATH_IS_NOT_DIRECTORY    = "The plocal path [%s] is not a directory.";

  /**
   * builds a StressTester object using the command line arguments
   *
   * @param args
   *
   * @return
   *
   * @throws Exception
   */
  public static OStressTester getStressTester(String[] args) throws Exception {

    final Map<String, String> options = checkOptions(readOptions(args));

    final OStressTesterSettings settings = new OStressTesterSettings();

    settings.dbName = TEMP_DATABASE_NAME + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
    if (options.get(OPTION_DBNAME) != null)
      settings.dbName = options.get(OPTION_DBNAME);

    settings.mode = OStressTester.OMode.valueOf(options.get(OPTION_MODE).toUpperCase());
    settings.rootPassword = options.get(OPTION_ROOT_PASSWORD);
    settings.resultOutputFile = options.get(OPTION_OUTPUT_FILE);
    settings.plocalPath = options.get(OPTION_PLOCAL_PATH);
    settings.operationsPerTransaction = getNumber(options.get(OPTION_TRANSACTIONS), "transactions");
    settings.delay = getNumber(options.get(OPTION_DELAY), "delay");
    settings.concurrencyLevel = getNumber(options.get(OPTION_CONCURRENCY), "concurrency");
    settings.remoteIp = options.get(OPTION_REMOTE_IP);
    settings.haMetrics = options.get(OPTION_HA_METRICS) != null ? Boolean.parseBoolean(options.get(OPTION_HA_METRICS)) : false;
    settings.workloadCfg = options.get(OPTION_WORKLOAD);
    settings.keepDatabaseAfterTest = options.get(OPTION_KEEP_DATABASE_AFTER_TEST) != null ?
        Boolean.parseBoolean(options.get(OPTION_KEEP_DATABASE_AFTER_TEST)) :
        false;
    settings.remotePort = 2424;
    settings.checkDatabase = Boolean.parseBoolean(options.get(OPTION_CHECK_DATABASE));
    if (options.get(OPTION_LOAD_BALANCING) != null)
      settings.loadBalancing = OStorageRemote.CONNECTION_STRATEGY.valueOf(options.get(OPTION_LOAD_BALANCING).toUpperCase());

    if (settings.plocalPath != null) {
      if (settings.plocalPath.endsWith(File.separator)) {
        settings.plocalPath = settings.plocalPath.substring(0, settings.plocalPath.length() - File.separator.length());
      }
      File plocalFile = new File(settings.plocalPath);
      if (!plocalFile.exists()) {
        throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_NOT_EXISTING_PLOCAL_PATH, settings.plocalPath));
      }
      if (!plocalFile.canWrite()) {
        throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_NO_WRITE_PERMISSION_PLOCAL_PATH, settings.plocalPath));
      }
      if (!plocalFile.isDirectory()) {
        throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_PLOCAL_PATH_IS_NOT_DIRECTORY, settings.plocalPath));
      }
    }

    if (settings.resultOutputFile != null) {

      File outputFile = new File(settings.resultOutputFile);
      if (outputFile.exists()) {
        outputFile.delete();
      }

      File parentFile = outputFile.getParentFile();

      // if the filename does not contain a path (both relative and absolute)
      if (parentFile == null) {
        parentFile = new File(".");
      }

      if (!parentFile.exists()) {
        throw new IllegalArgumentException(
            String.format(COMMAND_LINE_PARSER_NOT_EXISTING_OUTPUT_DIRECTORY, parentFile.getAbsoluteFile()));
      }
      if (!parentFile.canWrite()) {
        throw new IllegalArgumentException(
            String.format(COMMAND_LINE_PARSER_NO_WRITE_PERMISSION_OUTPUT_FILE, parentFile.getAbsoluteFile()));
      }
    }

    if (options.get(OPTION_REMOTE_PORT) != null) {
      settings.remotePort = getNumber(options.get(OPTION_REMOTE_PORT), "remotePort");
      if (settings.remotePort > 65535) {
        throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_INVALID_REMOTE_PORT_NUMBER, settings.remotePort));
      }
    }

    if (settings.mode == OStressTester.OMode.DISTRIBUTED) {
      throw new IllegalArgumentException(String.format("OMode [%s] not yet supported.", settings.mode));
    }

    if (settings.mode == OStressTester.OMode.REMOTE && settings.remoteIp == null) {
      throw new IllegalArgumentException(COMMAND_LINE_PARSER_MISSING_REMOTE_IP);
    }

    if (settings.rootPassword == null && settings.mode == OStressTester.OMode.REMOTE) {
      Console console = System.console();
      if (console != null) {
        settings.rootPassword = String
            .valueOf(console.readPassword(String.format(CONSOLE_REMOTE_PASSWORD_PROMPT, settings.remoteIp, settings.remotePort)));
      } else {
        throw new Exception(ERROR_OPENING_CONSOLE);
      }
    }

    final List<OWorkload> workloads = parseWorkloads(settings.workloadCfg);

    final ODatabaseIdentifier databaseIdentifier = new ODatabaseIdentifier(settings);

    return new OStressTester(workloads, databaseIdentifier, settings);
  }

  private static List<OWorkload> parseWorkloads(final String workloadConfig) {
    if (workloadConfig == null || workloadConfig.isEmpty())
      throw new IllegalArgumentException("Workload parameter is mandatory. Syntax: <workload-name:workload-params>");

    final List<OWorkload> result = new ArrayList<OWorkload>();

    final String[] parts = workloadConfig.split(",");
    for (String part : parts) {
      String workloadName;
      String workloadParams;

      final int pos = part.indexOf(":");
      if (pos > -1) {
        workloadName = part.substring(0, pos);
        workloadParams = part.substring(pos + 1);
      } else {
        workloadName = part;
        workloadParams = null;
      }

      final OWorkload workload = OStressTester.getWorkloadFactory().get(workloadName);
      if (workload == null)
        throw new IllegalArgumentException(
            "Workload '" + workloadName + "' is not configured. Use one of the following: " + OStressTester.getWorkloadFactory()
                .getRegistered());
      workload.parseParameters(workloadParams);

      result.add(workload);
    }

    return result;
  }

  private static int getNumber(final String value, final String option) throws IllegalArgumentException {
    if (value == null)
      return 0;

    try {
      int val = Integer.parseInt(value);
      if (val < 0) {
        throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_LESSER_THAN_ZERO_NUMBER, option));
      }
      return val;
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_INVALID_NUMBER, option, value));
    }
  }

  private static Map<String, String> checkOptions(Map<String, String> options) throws IllegalArgumentException {

    if (options.get(OPTION_MODE) == null) {
      throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_MODE_PARAM_MANDATORY));
    }

    options = setDefaultIfNotPresent(options, OPTION_MODE, OStressTester.OMode.PLOCAL.name());
    options = setDefaultIfNotPresent(options, OPTION_CONCURRENCY, "4");
    options = setDefaultIfNotPresent(options, OPTION_TRANSACTIONS, "0");
    options = setDefaultIfNotPresent(options, OPTION_WORKLOAD, "CRUD:C25000R25000U25000D25000");

    try {
      OStressTester.OMode.valueOf(options.get(OPTION_MODE).toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_INVALID_MODE, options.get(OPTION_MODE)));
    }

    return options;
  }

  private static Map<String, String> setDefaultIfNotPresent(Map<String, String> options, String option, String value)
      throws IllegalArgumentException {
    if (!options.containsKey(option)) {
      System.out.println(String.format("WARNING: '%s' option not found. Defaulting to %s.", option, value));
      options.put(option, value);
    }
    return options;
  }

  private static Map<String, String> readOptions(final String[] args) throws IllegalArgumentException {

    final Map<String, String> options = new HashMap<String, String>();

    // reads arguments from command line
    for (int i = 0; i < args.length; i++) {

      // an argument cannot be shorter than one char
      if (args[i].length() < 2) {
        throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_INVALID_OPTION, args[i]));
      }

      switch (args[i].charAt(0)) {
      case '-':
        if (args.length - 1 == i) {
          throw new IllegalArgumentException((String.format(COMMAND_LINE_PARSER_EXPECTED_VALUE, args[i])));
        }

        String option = args[i].substring(1);
        if (option.startsWith("-")) {
          option = option.substring(1);
        } else {
          if (!MAIN_OPTIONS.contains(option)) {
            throw new IllegalArgumentException((String.format(COMMAND_LINE_PARSER_INVALID_OPTION, args[i])));
          }
        }
        options.put(option, args[i + 1]);

        // jumps to the next switch
        i++;

        break;
      }
    }

    return options;
  }

}
