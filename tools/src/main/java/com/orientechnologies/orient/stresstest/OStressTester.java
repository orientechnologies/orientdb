/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.stresstest;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.stresstest.workload.OCheckWorkload;
import com.orientechnologies.orient.stresstest.workload.OWorkload;
import com.orientechnologies.orient.stresstest.workload.OWorkloadFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The main class of the OStressTester. It is instantiated from the OStressTesterCommandLineParser
 * and takes care of launching the needed threads (OOperationsExecutor) for executing the operations
 * of the test.
 *
 * @author Andrea Iacono
 */
public class OStressTester {
  /** The access mode to the database */
  public enum OMode {
    PLOCAL,
    MEMORY,
    REMOTE,
    DISTRIBUTED
  }

  private final ODatabaseIdentifier databaseIdentifier;
  private OConsoleProgressWriter consoleProgressWriter;
  private final OStressTesterSettings settings;

  private static final OWorkloadFactory workloadFactory = new OWorkloadFactory();
  private List<OWorkload> workloads = new ArrayList<OWorkload>();

  public OStressTester(
      final List<OWorkload> workloads,
      ODatabaseIdentifier databaseIdentifier,
      final OStressTesterSettings settings)
      throws Exception {
    this.workloads = workloads;
    this.databaseIdentifier = databaseIdentifier;
    this.settings = settings;
  }

  public static void main(String[] args) {
    System.out.println(
        String.format(
            "OrientDB Stress Tool v.%s - %s", OConstants.getVersion(), OConstants.COPYRIGHT));

    int returnValue = 1;
    try {
      final OStressTester stressTester = OStressTesterCommandLineParser.getStressTester(args);
      returnValue = stressTester.execute();
    } catch (Exception ex) {
      System.err.println(ex.getMessage());
    }
    System.exit(returnValue);
  }

  @SuppressWarnings("unchecked")
  public int execute() throws Exception {

    int returnCode = 0;

    // we don't want logs from DB
    OLogManager.instance().setConsoleLevel("SEVERE");

    // creates the temporary DB where to execute the test
    ODatabaseUtils.createDatabase(databaseIdentifier);
    System.out.println(String.format("Created database [%s].", databaseIdentifier.getUrl()));

    try {
      for (OWorkload workload : workloads) {
        consoleProgressWriter = new OConsoleProgressWriter("Console progress writer", workload);

        consoleProgressWriter.start();

        consoleProgressWriter.printMessage(
            String.format(
                "\nStarting workload %s (concurrencyLevel=%d)...",
                workload.getName(), settings.concurrencyLevel));

        final long startTime = System.currentTimeMillis();

        workload.execute(settings, databaseIdentifier);

        final long endTime = System.currentTimeMillis();

        consoleProgressWriter.sendShutdown();

        System.out.println(
            String.format(
                "\n- Total execution time: %.3f secs", ((float) (endTime - startTime) / 1000f)));

        System.out.println(workload.getFinalResult());

        dumpHaMetrics();

        if (settings.checkDatabase && workload instanceof OCheckWorkload) {
          System.out.println(String.format("- Checking database..."));
          ((OCheckWorkload) workload).check(databaseIdentifier);
          System.out.println(String.format("- Check completed"));
        }
      }

      if (settings.resultOutputFile != null) writeFile();

    } catch (Exception ex) {
      System.err.println(
          "\nAn error has occurred while running the stress test: " + ex.getMessage());
      returnCode = 1;
    } finally {
      // we don't need to drop the in-memory DB
      if (settings.keepDatabaseAfterTest || databaseIdentifier.getMode() == OMode.MEMORY)
        consoleProgressWriter.printMessage(
            String.format("\nDatabase is available on [%s].", databaseIdentifier.getUrl()));
      else {
        ODatabaseUtils.dropDatabase(databaseIdentifier);
        consoleProgressWriter.printMessage(
            String.format("\nDropped database [%s].", databaseIdentifier.getUrl()));
      }
    }

    return returnCode;
  }

  private void dumpHaMetrics() {
    if (settings.haMetrics) {
      final ODatabase db =
          ODatabaseUtils.openDatabase(
              databaseIdentifier, OStorageRemote.CONNECTION_STRATEGY.STICKY);
      try {
        final String output =
            db.command(new OCommandSQL("ha status -latency -messages -output=text")).execute();
        System.out.println("HA METRICS");
        System.out.println(output);

      } catch (Exception e) {
        // IGNORE IT
      } finally {
        db.close();
      }
    }
  }

  private void writeFile() {
    try {
      final StringBuilder output = new StringBuilder();
      output.append("{\"result\":[");
      int i = 0;
      for (OWorkload workload : workloads) {
        if (i++ > 0) output.append(",");
        output.append(workload.getFinalResultAsJson());
      }
      output.append("]}");

      OIOUtils.writeFile(new File(settings.resultOutputFile), output.toString());
    } catch (IOException e) {
      System.err.println("\nError on writing the result file : " + e.getMessage());
    }
  }

  public int getThreadsNumber() {
    return settings.concurrencyLevel;
  }

  public OMode getMode() {
    return databaseIdentifier.getMode();
  }

  public ODatabaseIdentifier getDatabaseIdentifier() {
    return databaseIdentifier;
  }

  public String getPassword() {
    return databaseIdentifier.getPassword();
  }

  public int getTransactionsNumber() {
    return settings.operationsPerTransaction;
  }

  public static OWorkloadFactory getWorkloadFactory() {
    return workloadFactory;
  }
}
