package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.*;

public class PerformanceSBTreeV3 {

    private static final int RUNS = 5;
    private static final int[] NUM_KEYS = {100000, 200000, 300000};

    private CellBTreeSingleValueV3<String> sbTreeV3;
    private OAtomicOperationsManager atomicOperationsManager;
    private OrientDB orientDB;
    private String dbName;

    public static void main(String[] args) throws Exception {
        Map<String, List<long[]>> results = new HashMap<>();

        for (int keys : NUM_KEYS) {
            PerformanceSBTreeV3 performanceTest = new PerformanceSBTreeV3();
            performanceTest.setUp();

            long totalTime1 = 0, totalTime2 = 0, totalTime3 = 0, totalTime4 = 0, totalTime5 = 0;
            long totalStorage1 = 0, totalStorage2 = 0, totalStorage3 = 0, totalStorage4 = 0, totalStorage5 = 0;

            for (int i = 0; i < RUNS; i++) {
                long startTime = System.currentTimeMillis();
                performanceTest.testKeyPut(keys);
                totalTime1 += System.currentTimeMillis() - startTime;
                totalStorage1 += performanceTest.getStorageSize();

                startTime = System.currentTimeMillis();
                performanceTest.testKeyPutRandomUniform(keys);
                totalTime2 += System.currentTimeMillis() - startTime;
                totalStorage2 += performanceTest.getStorageSize();

                startTime = System.currentTimeMillis();
                performanceTest.testKeyPutRandomGaussian(keys);
                totalTime3 += System.currentTimeMillis() - startTime;
                totalStorage3 += performanceTest.getStorageSize();

                startTime = System.currentTimeMillis();
                performanceTest.testKeyDelete(keys);
                totalTime4 += System.currentTimeMillis() - startTime;
                totalStorage4 += performanceTest.getStorageSize();

                startTime = System.currentTimeMillis();
                performanceTest.testKeyPutUniformDelete(keys);
                totalTime5 += System.currentTimeMillis() - startTime;
                totalStorage5 += performanceTest.getStorageSize();
            }

            addToResults(results, "testKeyPut", keys, totalTime1 / RUNS, totalStorage1 / RUNS);
            addToResults(results, "testKeyPutRandomUniform", keys, totalTime2 / RUNS, totalStorage2 / RUNS);
            addToResults(results, "testKeyPutRandomGaussian", keys, totalTime3 / RUNS, totalStorage3 / RUNS);

            addToResults(results, "testKeyDelete", keys,totalTime4 / RUNS, totalStorage4 / RUNS);
            addToResults(results, "testKeyPutUniformDelete", keys,totalTime5 / RUNS, totalStorage5 / RUNS);
            performanceTest.tearDown();
        }

        printResults(results);
    }

    private static void addToResults(Map<String, List<long[]>> results, String testName, int keys, long time, long storage) {
        long[] data = {keys, time, storage};
        if (!results.containsKey(testName)) {
            results.put(testName, new ArrayList<>());
        }
        results.get(testName).add(data);
    }

    private static void printResults(Map<String, List<long[]>> results) {
        System.out.println(String.format("%-30s %-15s %-15s %-15s", "Test", "Keys", "Execution Time (ms)", "Storage Size (bytes)"));
        for (String testName : results.keySet()) {
            for (long[] data : results.get(testName)) {
                System.out.println(String.format("%-30s %-15d %-15d %-15d", testName, data[0], data[1], data[2]));
            }
        }
    }

    public void setUp() throws Exception {
        final String buildDirectory = System.getProperty("buildDirectory", ".")
                + File.separator
                + PerformanceSBTreeV3.class.getSimpleName();

        dbName = "localSingleBTreeTest";
        final File dbDirectory = new File(buildDirectory, dbName);

        final OrientDBConfig config = OrientDBConfig.builder().build();
        orientDB = new OrientDB("plocal:" + buildDirectory, config);
        orientDB.create(dbName, ODatabaseType.PLOCAL);

        OAbstractPaginatedStorage storage;
        try (ODatabaseSession db = orientDB.open(dbName, "admin", "admin")) {
            storage = (OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db).getStorage();
        }

        sbTreeV3 = new CellBTreeSingleValueV3<>("singleBTree", ".sbt", ".nbt", storage);
        atomicOperationsManager = storage.getAtomicOperationsManager();
        atomicOperationsManager.executeInsideAtomicOperation(
                null,
                atomicOperation ->
                        sbTreeV3.create(atomicOperation, OUTF8Serializer.INSTANCE, null, 1, null)
        );
    }

    public void tearDown() {
        dbName = "localSingleBTreeTest";
        orientDB.drop(dbName);
        orientDB.close();
    }

    public long getStorageSize() {
        String buildDirectory = System.getProperty("buildDirectory", ".")
                + File.separator
                + PerformanceSBTreeV3.class.getSimpleName();

        File dbDirectory = new File(buildDirectory, dbName);

        return getDirectorySize(dbDirectory);
    }

    private static long getDirectorySize(File directory) {
        long length = 0;
        for (File file : directory.listFiles()) {
            if (file.isFile()) {
                length += file.length();
            } else {
                length += getDirectorySize(file);
            }
        }
        return length;
    }

    public void testKeyPut(int keysCount) throws Exception {
        atomicOperationsManager.executeInsideAtomicOperation(
                null,
                atomicOperation -> {
                    for (int i = 0; i < keysCount; i++) {
                        sbTreeV3.put(atomicOperation, Integer.toString(i), new ORecordId(i % 32000, i));
                    }
                }
        );
    }

    public void testKeyPutRandomUniform(int keysCount) throws Exception {
        final Random random = new Random();

        atomicOperationsManager.executeInsideAtomicOperation(
                null,
                atomicOperation -> {
                    for (int i = 0; i < keysCount; i++) {
                        int val = random.nextInt(Integer.MAX_VALUE);
                        sbTreeV3.put(atomicOperation, Integer.toString(val), new ORecordId(val % 32000, val));
                    }
                }
        );
    }

    public void testKeyPutRandomGaussian(int keysCount) throws Exception {
        Random random = new Random();

        atomicOperationsManager.executeInsideAtomicOperation(
                null,
                atomicOperation -> {
                    for (int i = 0; i < keysCount; i++) {
                        int val;
                        do {
                            val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
                        } while (val < 0);
                        sbTreeV3.put(atomicOperation, Integer.toString(val), new ORecordId(val % 32000, val));
                    }
                }
        );
    }

    public void testKeyDelete(int keysCount) throws Exception {
        for (int i = 0; i < keysCount; i++) {
            final int k = i;
            atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation ->
                    sbTreeV3.put(atomicOperation, Integer.toString(k),
                            new ORecordId(k % 32000, k)));
        }

        for (int i = 0; i < keysCount; i++) {
            final int iteration = i;
            atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation -> {
                if (iteration % 3 == 0) {
                    sbTreeV3.remove(atomicOperation, Integer.toString(iteration));
                }
            });
        }
    }

    public void testKeyPutUniformDelete(int keysCount) throws IOException, IOException {
        final TreeMap<String, ORID> keys = new TreeMap<>();
        final long seed = System.nanoTime();
        final Random random = new Random(seed);

        while (keys.size() < keysCount) {
            atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation -> {
                int val = random.nextInt(Integer.MAX_VALUE);
                String key = Integer.toString(val);
                sbTreeV3.put(atomicOperation, key, new ORecordId(val % 32000, val));
                keys.put(key, new ORecordId(val % 32000, val));
            });
        }

        for (final String key : keys.keySet()) {
            final int val = Integer.parseInt(key);
            if (val % 3 == 0) {
                atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation ->
                        sbTreeV3.remove(atomicOperation, key));
            }
        }
    }

}
