package com.orientechnologies.common.util;

import java.util.*;

/**
 * Fetches list of SQL queries are running at the moment into the OrientDB
 */
public class OSQLDumper {
  private static final String COMMAND_START = "<command>";
  private static final String COMMAND_END   = "</command>";

  private static final String QUERY_START = "<query>";
  private static final String QUERY_END   = "</query>";

  /**
   * @return list of SQL queries are running at the moment into the OrientDB
   */
  public static Collection<String> dumpAllSQLQueries() {
    final List<String> runningQueries = new ArrayList<String>();

    try {
      final Set<Thread> threads = Thread.getAllStackTraces().keySet();
      for (Thread thread : threads) {
        final String name = thread.getName();
        if (name != null) {
          final int queryStart = name.indexOf(QUERY_START);

          if (queryStart == -1) {
            final int commandStart = name.indexOf(COMMAND_START);

            if (commandStart != -1) {
              final int commandEnd = name.indexOf(COMMAND_END);
              if (commandEnd != -1) {
                final String command = name.substring(commandStart + COMMAND_START.length(), commandEnd);
                runningQueries.add(command);
              }
            }
          } else {
            final int queryEnd = name.indexOf(QUERY_END);
            if (queryEnd != -1) {
              final String query = name.substring(queryStart + QUERY_START.length(), queryEnd);
              runningQueries.add(query);
            }
          }
        }
      }
    } catch (SecurityException e) {
      return Collections.emptyList();
    }

    return runningQueries;
  }
}
