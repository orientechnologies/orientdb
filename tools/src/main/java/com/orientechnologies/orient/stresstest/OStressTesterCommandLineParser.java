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

import com.orientechnologies.orient.stresstest.operations.OOperationsSet;
import com.orientechnologies.orient.stresstest.util.OConstants;
import com.orientechnologies.orient.stresstest.util.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.util.OErrorMessages;
import com.orientechnologies.orient.stresstest.util.OInitException;

import java.io.Console;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the parser of the command line arguments passed with the invocation of OStressTester.
 * It contains a static method that - given the arguments - returns a OStressTester object.
 *
 * @author Andrea Iacono
 */
public class OStressTesterCommandLineParser {

  /**
   * builds a StressTester object using the command line arguments
   *
   * @param args
   * @return
   * @throws Exception
   */
  public static OStressTester getStressTester(String[] args) throws Exception {

    Map<String, String> options = checkOptions(readOptions(args));
    String dbName = OConstants.TEMP_DATABASE_NAME + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
    OMode mode = OMode.valueOf(options.get(OConstants.OPTION_MODE).toUpperCase());
    String rootPassword = options.get(OConstants.OPTION_ROOT_PASSWORD);
    String resultOutputFile = options.get(OConstants.OPTION_OUTPUT_FILE);
    String plocalPath = options.get(OConstants.OPTION_PLOCAL_PATH);
    int iterationsNumber = getNumber(options.get(OConstants.OPTION_ITERATIONS), "iterations");
    int operationsPerTransaction = getNumber(options.get(OConstants.OPTION_TRANSACTIONS), "transactions");
    int threadsNumber = getNumber(options.get(OConstants.OPTION_THREADS), "threads");
    OOperationsSet operationsSet = new OOperationsSet(options.get(OConstants.OPTION_OPERATIONS), threadsNumber, iterationsNumber);
    String remoteIp = options.get(OConstants.OPTION_REMOTE_IP);
    int remotePort = 2424;

    if (plocalPath!= null) {
      if (plocalPath.endsWith(File.separator)) {
        plocalPath = plocalPath.substring(0, plocalPath.length() - File.separator.length());
      }
      File plocalFile = new File(plocalPath);
      if (!plocalFile.exists()) {
        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_NOT_EXISTING_PLOCAL_PATH, plocalPath));
      }
      if (!plocalFile.canWrite()) {
        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_NO_WRITE_PERMISSION_PLOCAL_PATH, plocalPath));
      }
      if (!plocalFile.isDirectory()) {
        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_PLOCAL_PATH_IS_NOT_DIRECTORY, plocalPath));
      }
    }

    if (resultOutputFile != null) {

      File outputFile = new File(resultOutputFile);
      if (outputFile.exists()) {
        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_EXISTING_OUTPUT_FILE, resultOutputFile));
      }

      File parentFile = outputFile.getParentFile();

      // if the filename does not contain a path (both relative and absolute)
      if (parentFile == null) {
        parentFile = new File(".");
      }

      if (!parentFile.exists()) {
        throw new OInitException(
            String.format(OErrorMessages.COMMAND_LINE_PARSER_NOT_EXISTING_OUTPUT_DIRECTORY, parentFile.getAbsoluteFile()));
      }
      if (!parentFile.canWrite()) {
        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_NO_WRITE_PERMISSION_OUTPUT_FILE,
            parentFile.getAbsoluteFile()));
      }
    }

    if (operationsPerTransaction > operationsSet.getNumberOfCreates()) {
      throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_TX_GREATER_THAN_CREATES, operationsPerTransaction,
          operationsSet.getNumberOfCreates()));
    }

    if (options.get(OConstants.OPTION_REMOTE_PORT) != null) {
      remotePort = getNumber(options.get(OConstants.OPTION_REMOTE_PORT), "remotePort");
      if (remotePort > 65535) {
        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_INVALID_REMOTE_PORT_NUMBER, remotePort));
      }
    }

    if (mode == OMode.DISTRIBUTED) {
      throw new OInitException(String.format("OMode [%s] not yet supported.", mode));
    }

    if (mode == OMode.REMOTE && remoteIp == null) {
      throw new OInitException(OErrorMessages.COMMAND_LINE_PARSER_MISSING_REMOTE_IP);
    }

    if (rootPassword == null && mode == OMode.REMOTE) {
      Console console = System.console();
      if (console != null) {
        rootPassword = String
            .valueOf(console.readPassword(String.format(OConstants.CONSOLE_REMOTE_PASSWORD_PROMPT, remoteIp, remotePort)));
      } else {
        throw new Exception(OErrorMessages.ERROR_OPENING_CONSOLE);
      }
    }

    ODatabaseIdentifier databaseIdentifier = new ODatabaseIdentifier(mode, dbName, rootPassword, remoteIp, remotePort, plocalPath);
    return new OStressTester(databaseIdentifier, operationsSet, iterationsNumber, threadsNumber, operationsPerTransaction,
        resultOutputFile);
  }

  private static int getNumber(String value, String option) throws OInitException {
    try {
      int val = Integer.parseInt(value);
      if (val < 0) {
        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_LESSER_THAN_ZERO_NUMBER, option));
      }
      return val;
    } catch (NumberFormatException ex) {
      throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_INVALID_NUMBER, option, value));
    }
  }

  private static Map<String, String> checkOptions(Map<String, String> options) throws OInitException {

    if (options.get(OConstants.OPTION_MODE) == null) {
      throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_MODE_PARAM_MANDATORY));
    }

    options = setDefaultIfNotPresent(options, OConstants.OPTION_MODE, OMode.PLOCAL.name());
    options = setDefaultIfNotPresent(options, OConstants.OPTION_ITERATIONS, "10");
    options = setDefaultIfNotPresent(options, OConstants.OPTION_THREADS, "4");
    options = setDefaultIfNotPresent(options, OConstants.OPTION_TRANSACTIONS, "0");
    options = setDefaultIfNotPresent(options, OConstants.OPTION_OPERATIONS, "C5000R5000U5000D5000");

    try {
      OMode.valueOf(options.get(OConstants.OPTION_MODE).toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_INVALID_MODE, options.get(OConstants.OPTION_MODE)));
    }

    return options;
  }

  private static Map<String, String> setDefaultIfNotPresent(Map<String, String> options, String option, String value)
      throws OInitException {
    if (!options.containsKey(option)) {
      System.out.println(String.format("WARNING: '%s' option not found. Defaulting to %s.", option, value));
      options.put(option, value);
    }
    return options;
  }

  private static Map<String, String> readOptions(String[] args) throws OInitException {

    Map<String, String> options = new HashMap<String, String>();

    // reads arguments from command line
    for (int i = 0; i < args.length; i++) {

      // an argument cannot be shorter than one char
      if (args[i].length() < 2) {
        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_INVALID_OPTION, args[i]));
      }

      switch (args[i].charAt(0)) {
      case '-':
        if (args.length - 1 == i) {
          throw new OInitException((String.format(OErrorMessages.COMMAND_LINE_PARSER_EXPECTED_VALUE, args[i])));
        }

        String option = args[i].substring(1);
        if (option.startsWith("-")) {
          option = option.substring(1);
        } else {
          if (!OConstants.MAIN_OPTIONS.contains(option)) {
            throw new OInitException((String.format(OErrorMessages.COMMAND_LINE_PARSER_INVALID_OPTION, args[i])));
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
