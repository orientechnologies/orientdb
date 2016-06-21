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
package com.orientechnologies.orient.stresstest.operations;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.stresstest.output.OConsoleProgressWriter;
import com.orientechnologies.orient.stresstest.output.OStressTestResults;
import com.orientechnologies.orient.stresstest.util.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.util.ODatabaseUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This class is the one that executes the operations of the test.
 * There will be as many instances of this class as the defined number of threads.
 *
 * @author Andrea Iacono
 */
public class OOperationsExecutor implements Callable {

  private OOperationsSet         operationsSet;
  private OConsoleProgressWriter consoleProgressWriter;
  private OStressTestResults     stressTestResults;
  private int                    txNumber;
  private int                    threads;
  private ODatabaseIdentifier    databaseIdentifier;

  public OOperationsExecutor(ODatabaseIdentifier databaseIdentifier, OOperationsSet operationsSet, int txNumber, int threads,
      OConsoleProgressWriter consoleProgressWriter, OStressTestResults stressTestResults) {
    this.databaseIdentifier = databaseIdentifier;
    this.txNumber = txNumber;
    this.threads = threads;
    this.operationsSet = operationsSet;
    this.consoleProgressWriter = consoleProgressWriter;
    this.stressTestResults = stressTestResults;
  }

  @Override public Object call() throws Exception {

    Map<OOperationType, Long> times = new HashMap<OOperationType, Long>();

    // the database must be opened in the executing thread
    ODatabase database = ODatabaseUtils.openDatabase(databaseIdentifier);

    // executes all the operations defined for this test
    long start = System.currentTimeMillis();
    List<OIdentifiable> insertedDocs = executeCreates(operationsSet.getNumber(OOperationType.CREATE) / threads, txNumber, database);
    times.put(OOperationType.CREATE, System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    executeReads(operationsSet.getNumber(OOperationType.READ) / threads, database);
    times.put(OOperationType.READ, System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    executeUpdates(operationsSet.getNumber(OOperationType.UPDATE) / threads, txNumber, database, insertedDocs);
    times.put(OOperationType.UPDATE, System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    executeDeletes(operationsSet.getNumber(OOperationType.DELETE) / threads, txNumber, database, insertedDocs);
    times.put(OOperationType.DELETE, System.currentTimeMillis() - start);

    consoleProgressWriter.stopProgress();

    // and return the timings of execution the test
    return times;
  }

  // an helper method for all the operations types
  private List<OIdentifiable> executeOperations(OOperationType operationType, long numberOfOperations, int operationsInTx,
      ODatabase database, Invokable<ODocument, Integer> operationToExecute) throws Exception {

    // divides the set of operations into smaller sets (to compute percentile)
    int percentileFrequency = operationsInTx > 0 ? operationsInTx : ((int) (numberOfOperations / 100));
    if( percentileFrequency == 0 )
      percentileFrequency = 1;

    List<OIdentifiable> insertedDocs = new ArrayList<OIdentifiable>();
    if (operationsInTx > 0) {
      database.begin();
    }
    long percentileStartTiming = System.currentTimeMillis();
    for (int j = 1; j <= numberOfOperations; j++) {

      if (operationsInTx > 0 && j % operationsInTx == 0) {
        database.commit();
        database.begin();
      }

      ODocument doc = operationToExecute.invoke(j - 1);

      if (j % percentileFrequency == 0) {
        stressTestResults.addPartialResult(operationType, System.currentTimeMillis() - percentileStartTiming);
        stressTestResults.setTestProgress(operationType, percentileFrequency);
        percentileStartTiming = System.currentTimeMillis();
      }
      if (doc != null) {
        insertedDocs.add(doc.getIdentity().copy());
      }
    }
    if (operationsInTx > 0) {
      database.commit();
      stressTestResults.setTestProgress(operationType, (int) (numberOfOperations % percentileFrequency));
    }
    return insertedDocs;
  }

  private List<OIdentifiable> executeCreates(long number, int operationsInTx, ODatabase database) throws Exception {
    if (number == 0) {
      return new ArrayList<OIdentifiable>();
    }
    return executeOperations(OOperationType.CREATE, number, operationsInTx, database, new Invokable<ODocument, Integer>() {
      @Override public ODocument invoke(Integer value) {
        return ODatabaseUtils.createOperation(value);
      }
    });
  }

  private void executeReads(long number, final ODatabase database) throws Exception {
    if (number == 0) {
      return;
    }
    executeOperations(OOperationType.READ, number, 0, database, new Invokable<ODocument, Integer>() {
      @Override public ODocument invoke(Integer value) throws Exception {
        ODatabaseUtils.readOperation(database, value);
        return null;
      }
    });
  }

  private void executeUpdates(long number, int operationsInTx, final ODatabase database, final List<OIdentifiable> insertedDocs)
      throws Exception {
    if (number == 0) {
      return;
    }
    executeOperations(OOperationType.UPDATE, number, operationsInTx, database, new Invokable<ODocument, Integer>() {
      @Override public ODocument invoke(Integer value) throws Exception {
        ODatabaseUtils.updateOperation(insertedDocs.get(value % insertedDocs.size()), value);
        return null;
      }
    });
  }

  private void executeDeletes(long number, int operationsInTx, final ODatabase database, final List<OIdentifiable> insertedDocs)
      throws Exception {
    if (number == 0) {
      return;
    }
    executeOperations(OOperationType.DELETE, number, operationsInTx, database, new Invokable<ODocument, Integer>() {
      @Override public ODocument invoke(Integer value) throws Exception {
        ODatabaseUtils.deleteOperation(insertedDocs.get(value));
        return null;
      }
    });
  }

  // a new interface because I need to throw an exception
  interface Invokable<T, V> {
    T invoke(V param) throws Exception;
  }

}
