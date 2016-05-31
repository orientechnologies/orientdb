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

    private OOperationsSet operationsSet;
    private String dbName;
    private String password;
    private OConsoleWriter consoleWriter;
    private List<ODocument> insertedDocs;

    public OOperationsExecutor(String dbName, String password, OOperationsSet operationsSet, OConsoleWriter consoleWriter) {
        this.dbName = dbName;
        this.password = password;
        this.consoleWriter = consoleWriter;
        this.operationsSet = operationsSet;
        insertedDocs = new ArrayList<>();
    }


    @Override
    public Object call() throws Exception {

        // the database must be opened in the executing thread
        ODatabase database = ODatabaseUtils.openDatabase(dbName, password);

        // executes all the operations defined for this test
        long start = System.currentTimeMillis();
        executeCreates(operationsSet.getNumberOfCreates());
        long createsTime = (System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        executeReads(operationsSet.getNumberOfReads(), database);
        long insertsTime = (System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        executeUpdates(operationsSet.getNumberOfUpdates());
        long updatesTime = (System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        executeDeletes(operationsSet.getNumberOfDeletes());
        long deletesTime = (System.currentTimeMillis() - start);

        // and return the timings of this run of the test
        return new OOperationsExecutorResults(
                createsTime,
                insertsTime,
                updatesTime,
                deletesTime);
    }

    private void executeCreates(int number) {
        for (int j = 0; j < number; j++) {
            insertedDocs.add(ODatabaseUtils.createOperation(j));
            consoleWriter.addCreate();
        }
    }

    private void executeReads(int number, ODatabase database) throws Exception {
        for (int j = 0; j < number; j++) {
            ODatabaseUtils.readOperation(database, j);
            consoleWriter.addRead();
        }
    }

    private void executeUpdates(int number) {
        for (int j = 0; j < number; j++) {
            ODatabaseUtils.updateOperation(insertedDocs.get(j % insertedDocs.size()), j);
            consoleWriter.addUpdate();
        }
    }

    private void executeDeletes(int number) {
        for (int j = 0; j < number; j++) {
            ODatabaseUtils.deleteOperation(insertedDocs.get(j));
            consoleWriter.addDelete();
        }
    }

}
