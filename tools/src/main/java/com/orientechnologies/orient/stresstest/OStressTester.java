package com.orientechnologies.orient.stresstest;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.stresstest.operations.OOperationsExecutor;
import com.orientechnologies.orient.stresstest.operations.OOperationsSet;
import com.orientechnologies.orient.stresstest.output.OOperationsExecutorResults;
import com.orientechnologies.orient.stresstest.output.OStressTestResults;
import com.orientechnologies.orient.stresstest.output.OConsoleWriter;
import com.orientechnologies.orient.stresstest.util.OConstants;
import com.orientechnologies.orient.stresstest.util.ODatabaseUtils;


import java.io.Console;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * The main class of the OStressTester.
 * It is instantiated from the OStressTesterCommandLineParser and takes care of
 * launching the needed threads (OOperationsExecutor) for executing the operations
 * of the test.
 */
public class OStressTester {

    private int threadsNumber;
    private String password;
    private OMode mode;
    private OOperationsSet operationsSet;
    private int iterationsNumber;
    private OConsoleWriter consoleProgressWriter;
    private OStressTestResults stressTestResults;

    public OStressTester(OMode mode, OOperationsSet operationsSet, int iterationsNumber, int threadsNumber, String password) throws Exception {
        this.mode = mode;
        this.operationsSet = operationsSet;
        this.iterationsNumber = iterationsNumber;
        this.threadsNumber = threadsNumber;
        stressTestResults = new OStressTestResults(operationsSet, mode, threadsNumber, iterationsNumber);
        consoleProgressWriter = new OConsoleWriter(operationsSet, threadsNumber, iterationsNumber);

        this.password = password;
        if (password == null) {
            Console console = System.console();
            if (console != null) {
                this.password = new String(console.readPassword("Server Root Password: "));
            } else {
//                throw new Exception("An error has occurred opening the console.");
            }
        }

    }

    @SuppressWarnings("unchecked")
    private int execute() throws Exception {

        long startTime;
        long totalTime = 0;
        int returnCode = 0;

        // we don't want logs from DB
        OLogManager.instance().setConsoleLevel("SEVERE");

        // creates the temporary DB where to execute the test
        String dbName = OConstants.TEMP_DATABASE_NAME + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        ODatabaseUtils.createDatabase(dbName, mode, password);

        // opens the newly created db and creates an index on the class we're going to use
        try (ODatabase database = ODatabaseUtils.openDatabase(dbName, password)) {

            final OSchema schema = database.getMetadata().getSchema();
            final OClass oClass = schema.createClass(OConstants.CLASS_NAME);
            oClass.createProperty("name", OType.STRING);
            OIndexManager indexManager = database.getMetadata().getIndexManager();
            indexManager.createIndex(
                    OConstants.INDEX_NAME,
                    OClass.INDEX_TYPE.UNIQUE.toString(),
                    new OPropertyIndexDefinition(
                            OConstants.CLASS_NAME,
                            "name",
                            OType.STRING
                    ),
                    oClass.getClusterIds(),
                    null,
                    null);
        }

        // starts the test
        try {

            // iterates n times
            for (int i = 0; i < iterationsNumber; i++) {

                // creates the operations executors
                List<Callable<OOperationsExecutorResults>> operationsExecutors = new ArrayList<>();
                for (int j = 0; j < threadsNumber; j++) {
                    operationsExecutors.add(new OOperationsExecutor(dbName, password, operationsSet, consoleProgressWriter));
                }

                // starts parallel execution (blocking)
                startTime = System.currentTimeMillis();
                List<Future<OOperationsExecutorResults>> threadsResults = Executors.newFixedThreadPool(threadsNumber).invokeAll(operationsExecutors);
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

        } catch (Exception ex) {
            System.err.println("\nAn error has occurred while running the stress test: " + ex.getMessage());
            returnCode = 1;
            // ex.printStackTrace();
        } finally {
            ODatabaseUtils.dropDatabase(dbName, mode, password);
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
        return mode;
    }

    public String getPassword() {
        return password;
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
