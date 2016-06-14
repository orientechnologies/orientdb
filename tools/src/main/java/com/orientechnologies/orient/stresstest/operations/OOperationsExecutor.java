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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.stresstest.output.OConsoleWriter;
import com.orientechnologies.orient.stresstest.output.OOperationsExecutorResults;
import com.orientechnologies.orient.stresstest.util.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.util.ODatabaseUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * This class is the one that executes the operations of the test. There will be as many instances of this class as the defined number of threads.
 *
 * @author Andrea Iacono
 */
public class OOperationsExecutor implements Callable {

  private OOperationsSet      operationsSet;
  private int                 txNumber;
  private OConsoleWriter      consoleWriter;
  private ODatabaseIdentifier databaseIdentifier;

  public OOperationsExecutor(ODatabaseIdentifier databaseIdentifier, OOperationsSet operationsSet, int txNumber,
      OConsoleWriter consoleWriter) {
    this.databaseIdentifier = databaseIdentifier;
    this.txNumber = txNumber;
    this.consoleWriter = consoleWriter;
    this.operationsSet = operationsSet;
  }

  @Override
  public Object call() throws Exception {

    // the database must be opened in the executing thread
    ODatabase database = ODatabaseUtils.openDatabase(databaseIdentifier);

    // executes all the operations defined for this test
    long start = System.currentTimeMillis();
    List<ODocument> insertedDocs = executeCreates(operationsSet.getNumberOfCreates(), txNumber, database);
    long createsTime = (System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    executeReads(operationsSet.getNumberOfReads(), database);
    long insertsTime = (System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    executeUpdates(operationsSet.getNumberOfUpdates(), txNumber, database, insertedDocs);
    long updatesTime = (System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    executeDeletes(operationsSet.getNumberOfDeletes(), txNumber, database, insertedDocs);
    long deletesTime = (System.currentTimeMillis() - start);
    
    consoleWriter.updateConsole();

    // and return the timings of this run of the test
    return new OOperationsExecutorResults(createsTime, insertsTime, updatesTime, deletesTime);
  }

  private List<ODocument> executeOperations(int number, int operationsInTx, ODatabase database,
      Invokable<ODocument, Integer, ODocument> operationToExecute) throws Exception {

    List<ODocument> insertedDocs = new ArrayList<ODocument>();
    int txCounter = 1;
    if (operationsInTx > 0) {
      database.begin();
    }
    for (int j = 0; j < number; j++) {
      if (operationsInTx > 0 && txCounter % operationsInTx == 0) {
        database.commit();
        database.begin();
      }
      ODocument doc = operationToExecute.invoke(j, null);
      if (doc != null) {
        insertedDocs.add(doc);
      }
    }
    if (operationsInTx > 0) {
      database.commit();
    }
    return insertedDocs;
  }

  private List<ODocument> executeCreates(int number, int operationsInTx, ODatabase database) throws Exception {
    return executeOperations(number, operationsInTx, database, new Invokable<ODocument, Integer, ODocument>() {
      @Override
      public ODocument invoke(Integer value, ODocument doc) {
        consoleWriter.addCreate();
        return ODatabaseUtils.createOperation(value);
      }
    });
  }

  private void executeReads(int number, final ODatabase database) throws Exception {
    executeOperations(number, 0, database, new Invokable<ODocument, Integer, ODocument>() {
      @Override
      public ODocument invoke(Integer value, ODocument doc) throws Exception {
        ODatabaseUtils.readOperation(database, value);
        consoleWriter.addRead();
        return null;
      }
    });
  }

  private void executeUpdates(int number, int operationsInTx, final ODatabase database, final List<ODocument> insertedDocs) throws Exception {
    executeOperations(number, operationsInTx, database, new Invokable<ODocument, Integer, ODocument>() {
      @Override
      public ODocument invoke(Integer value, ODocument doc) throws Exception {
        ODatabaseUtils.updateOperation(insertedDocs.get(value % insertedDocs.size()), value);
        consoleWriter.addUpdate();
        return null;
      }
    });
  }

  private void executeDeletes(int number, int operationsInTx, final ODatabase database, final List<ODocument> insertedDocs) throws Exception {
    executeOperations(number, operationsInTx, database, new Invokable<ODocument, Integer, ODocument>() {
      @Override
      public ODocument invoke(Integer value, ODocument doc) throws Exception {
        ODatabaseUtils.deleteOperation(insertedDocs.get(value));
        consoleWriter.addDelete();
        return null;
      }
    });
  }

  interface Invokable<T, V, Z> {
    T invoke(V first, Z second) throws Exception;
  }

}
