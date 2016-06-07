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

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.stresstest.operations.OOperationsExecutor;
import com.orientechnologies.orient.stresstest.operations.OOperationsSet;
import com.orientechnologies.orient.stresstest.output.OConsoleWriter;
import com.orientechnologies.orient.stresstest.output.OJsonResultsFormatter;
import com.orientechnologies.orient.stresstest.output.OOperationsExecutorResults;
import com.orientechnologies.orient.stresstest.output.OStressTestResults;
import com.orientechnologies.orient.stresstest.util.OConstants;
import com.orientechnologies.orient.stresstest.util.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.util.ODatabaseUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The main class of the OStressTester.
 * It is instantiated from the OStressTesterCommandLineParser and takes care of
 * launching the needed threads (OOperationsExecutor) for executing the operations
 * of the test.
 *
 * @author Andrea Iacono
 */
public class OStressTester {

  private int                 threadsNumber;
  private int                 iterationsNumber;
  private ODatabaseIdentifier databaseIdentifier;
  private int                 txNumber;
  private String              outputResultFile;
  private OOperationsSet      operationsSet;
  private OConsoleWriter      consoleProgressWriter;
  private OStressTestResults  stressTestResults;

  public OStressTester(ODatabaseIdentifier databaseIdentifier, OOperationsSet operationsSet, int iterationsNumber,
      int threadsNumber, int txNumber, String outputResultFile) throws Exception {
    this.operationsSet = operationsSet;
    this.iterationsNumber = iterationsNumber;
    this.threadsNumber = threadsNumber;
    this.databaseIdentifier = databaseIdentifier;
    this.txNumber = txNumber;
    this.outputResultFile = outputResultFile;
    stressTestResults = new OStressTestResults(operationsSet, databaseIdentifier.getMode(), threadsNumber, iterationsNumber);
    consoleProgressWriter = new OConsoleWriter(operationsSet, threadsNumber, iterationsNumber);
  }

  @SuppressWarnings("unchecked")
  private int execute() throws Exception {

    long startTime;
    long totalTime = 0;
    int returnCode = 0;

    // we don't want logs from DB
    OLogManager.instance().setConsoleLevel("SEVERE");

    // creates the temporary DB where to execute the test
    ODatabaseUtils.createDatabase(databaseIdentifier);
    consoleProgressWriter.printMessage(String.format("Created database [%s].", databaseIdentifier.getUrl()));

    // opens the newly created db and creates an index on the class we're going to use
    ODatabase database = ODatabaseUtils.openDatabase(databaseIdentifier);
    if (database == null) {
      throw new Exception("Couldn't open database " + databaseIdentifier.getName() + ".");
    }
    try {
      final OSchema schema = database.getMetadata().getSchema();
      final OClass oClass = schema.createClass(OConstants.CLASS_NAME);
      oClass.createProperty("name", OType.STRING);
      OIndexManager indexManager = database.getMetadata().getIndexManager();
      indexManager.createIndex(OConstants.INDEX_NAME, OClass.INDEX_TYPE.UNIQUE.toString(),
          new OPropertyIndexDefinition(OConstants.CLASS_NAME, "name", OType.STRING), oClass.getClusterIds(), null, null);
    } finally {
      database.close();
    }

    // starts the test
    try {

      // iterates n times
      for (int i = 0; i < iterationsNumber; i++) {

        // creates the operations executors
        List<Callable<OOperationsExecutorResults>> operationsExecutors = new ArrayList<Callable<OOperationsExecutorResults>>();
        for (int j = 0; j < threadsNumber; j++) {
          operationsExecutors.add(new OOperationsExecutor(databaseIdentifier, operationsSet, txNumber, consoleProgressWriter));
        }

        // starts parallel execution (blocking call)
        startTime = System.currentTimeMillis();
        List<Future<OOperationsExecutorResults>> threadsResults = Executors.newFixedThreadPool(threadsNumber)
            .invokeAll(operationsExecutors);
        totalTime += System.currentTimeMillis() - startTime;

        // add the output of every executor
        for (Future<OOperationsExecutorResults> threadResults : threadsResults) {
          stressTestResults.addThreadResults(threadResults.get());
        }
      }
      // stops total benchmarking
      stressTestResults.addTotalExecutionTime(totalTime);

      // prints out total output
      System.out.println("\r                                                                                             ");
      System.out.println(stressTestResults.toString());

      // if specified, writes output to file
      if (outputResultFile != null) {
        OIOUtils.writeFile(new File(outputResultFile), OJsonResultsFormatter.format(stressTestResults));
      }
    } catch (Exception ex) {
      System.err.println("\nAn error has occurred while running the stress test: " + ex.getMessage());
      returnCode = 1;
    } finally {
      // we don't need to drop the in-memory DB
      if (databaseIdentifier.getMode() != OMode.MEMORY) {
        ODatabaseUtils.dropDatabase(databaseIdentifier);
        consoleProgressWriter.printMessage(String.format("Dropped database [%s].", databaseIdentifier.getUrl()));
      }
    }

    return returnCode;
  }

  public int getIterationsNumber() {
    return iterationsNumber;
  }

  public int getThreadsNumber() {
    return threadsNumber;
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
    return txNumber;
  }

  public static void main(String[] args) {
    int returnValue = 1;
    try {
      OStressTester stressTester = OStressTesterCommandLineParser.getStressTester(args);
      returnValue = stressTester.execute();
    } catch (Exception ex) {
      System.err.println(ex.getMessage());
    }
    System.exit(returnValue);
  }

}
