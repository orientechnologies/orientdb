package com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.KeyNormalizers;
import org.junit.Assert;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.io.File;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Random;

public class PerformanceBinaryBTree {

    private static final int RUNS = 5;
    private static final int[] NUM_KEYS = {100000, 200000, 300000};

    private OAtomicOperationsManager atomicOperationsManager;
    private BinaryBTree binaryBTree;
    private OrientDB orientDB;
    private String dbName;
    private OAbstractPaginatedStorage storage;
    private int keysCount;
    private static KeyNormalizers keyNormalizers = new KeyNormalizers(Locale.ENGLISH, Collator.NO_DECOMPOSITION);
    private static OType[] types = new OType[]{OType.STRING};

    public static void main(String[] args) throws Exception {
        Map<String, List<long[]>> results = new HashMap<>();

        for (int keys : NUM_KEYS) {
            PerformanceBinaryBTree performanceTest = new PerformanceBinaryBTree();
            performanceTest.setUp(keys);

            long totalTime1 = 0, totalTime2 = 0, totalTime3 = 0, totalTime4 = 0, totalTime5 = 0;
            long totalStorage1 = 0, totalStorage2 = 0, totalStorage3 = 0, totalStorage4 = 0, totalStorage5 = 0;

            for (int i = 0; i < RUNS; i++) {
                long startTime = System.currentTimeMillis();
                performanceTest.testKeyPut();
                totalTime1 += System.currentTimeMillis() - startTime;
                totalStorage1 += performanceTest.getStorageSize();

                startTime = System.currentTimeMillis();
                performanceTest.testKeyPutRandomUniform();
                totalTime2 += System.currentTimeMillis() - startTime;
                totalStorage2 += performanceTest.getStorageSize();

                startTime = System.currentTimeMillis();
                performanceTest.testKeyPutRandomGaussian();
                totalTime3 += System.currentTimeMillis() - startTime;
                totalStorage3 += performanceTest.getStorageSize();

                startTime = System.currentTimeMillis();
                performanceTest.testKeyDelete();
                totalTime4 += System.currentTimeMillis() - startTime;
                totalStorage4 += performanceTest.getStorageSize();

                startTime = System.currentTimeMillis();
                performanceTest.testKeyPutUniformDelete();
                totalTime5 += System.currentTimeMillis() - startTime;
                totalStorage5 += performanceTest.getStorageSize();
            }

            addToResults(results, "testKeyPut", totalTime1 / RUNS, totalStorage1 / RUNS);
            addToResults(results, "testKeyPutRandomUniform", totalTime2 / RUNS, totalStorage2 / RUNS);
            addToResults(results, "testKeyPutRandomGaussian", totalTime3 / RUNS, totalStorage3 / RUNS);
            addToResults(results, "testKeyDelete", totalTime4 / RUNS, totalStorage4 / RUNS);
            addToResults(results, "testKeyPutUniformDelete", totalTime5 / RUNS, totalStorage5 / RUNS);
            performanceTest.tearDown();
        }

        printResults(results);
    }
    private static void addToResults(Map<String, List<long[]>> results, String testName, long time, long storage) {
        long[] data = {time, storage};
        if (!results.containsKey(testName)) {
            results.put(testName, new ArrayList<>());
        }
        results.get(testName).add(data);
    }

    private static void printResults(Map<String, List<long[]>> results) {
        System.out.print("Method");
        for (int keys : NUM_KEYS) {
            System.out.printf("\t%d time (ms)\t%d storage (bytes)", keys, keys);
        }
        System.out.println();

        for (String testName : results.keySet()) {
            System.out.print(testName);
            for (long[] data : results.get(testName)) {
                System.out.printf("\t%d\t%d", data[0], data[1]);
            }
            System.out.println();
        }
    }
    public void setUp(int keysCount) throws Exception {
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.setValue(4);
        OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.setValue(1024);

        final String buildDirectory = System.getProperty("buildDirectory", ".") + File.separator + BinaryBTree.class.getSimpleName();
        dbName = "bBTreeTest" + keysCount;
        final File dbDirectory = new File(buildDirectory, dbName);

        final OrientDBConfig config = OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE, 4)
                .addConfig(OGlobalConfiguration.SBTREE_MAX_KEY_SIZE, 1024)
                .build();

        orientDB = new OrientDB("plocal:" + buildDirectory, config);
        orientDB.create(dbName, ODatabaseType.PLOCAL);

        try (ODatabaseSession databaseDocumentTx = orientDB.open(dbName, "admin", "admin")) {
            storage = (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseDocumentTx).getStorage();
        }
        binaryBTree = new BinaryBTree(1, 1024, 16, storage, "singleBTree", ".bbt");
        atomicOperationsManager = storage.getAtomicOperationsManager();
        atomicOperationsManager.executeInsideAtomicOperation(null, binaryBTree::create);

        this.keysCount = keysCount;
    }

    public void tearDown() {
        orientDB.drop(dbName);
        orientDB.close();
    }
    public long getStorageSize() {
        String buildDirectory = System.getProperty("buildDirectory", ".")
                + File.separator
                + BinaryBTree.class.getSimpleName();
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

    public void testKeyPut() throws Exception {


        for (int i = 0; i < keysCount; i++) {
            final int iteration = i;
            atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation -> {
                final String key = Integer.toString(iteration);
                binaryBTree.put(atomicOperation, stringToLexicalBytes(key), new ORecordId(iteration % 32000, iteration));
            });
        }
    }

    public void testKeyPutRandomUniform() throws Exception {
        final Random random = new Random();

        for (int i = 0; i < keysCount; i++) {
            atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation -> {
                int val = random.nextInt(Integer.MAX_VALUE);
                String key = Integer.toString(val);
                binaryBTree.put(atomicOperation, stringToLexicalBytes(key), new ORecordId(val % 32000, val));
            });
        }
    }

    public void testKeyPutRandomGaussian() throws Exception {
        Random random = new Random();

        for (int i = 0; i < keysCount; i++) {
            atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation -> {
                int val;
                do {
                    val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
                } while (val < 0);

                final String key = Integer.toString(val);
                binaryBTree.put(atomicOperation, stringToLexicalBytes(key), new ORecordId(val % 32000, val));
            });
        }
    }

    public void testKeyDelete() throws Exception {
        for (int i = 0; i < keysCount; i++) {
            final int k = i;
            atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation ->
                    binaryBTree.put(atomicOperation, stringToLexicalBytes(Integer.toString(k)),
                            new ORecordId(k % 32000, k)));
        }

        for (int i = 0; i < keysCount; i++) {
            final int iteration = i;
            atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation -> {
                if (iteration % 3 == 0) {
                    binaryBTree.remove(atomicOperation, stringToLexicalBytes(Integer.toString(iteration)));
                }
            });
        }
    }

    public void testKeyPutUniformDelete() throws IOException, IOException {
        final TreeMap<String, ORID> keys = new TreeMap<>();
        final long seed = System.nanoTime();
        final Random random = new Random(seed);

        while (keys.size() < keysCount) {
            atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation -> {
                int val = random.nextInt(Integer.MAX_VALUE);
                String key = Integer.toString(val);
                binaryBTree.put(atomicOperation, stringToLexicalBytes(key), new ORecordId(val % 32000, val));
                keys.put(key, new ORecordId(val % 32000, val));
            });
        }

        for (final String key : keys.keySet()) {
            final int val = Integer.parseInt(key);
            if (val % 3 == 0) {
                atomicOperationsManager.executeInsideAtomicOperation(null, atomicOperation ->
                        binaryBTree.remove(atomicOperation, stringToLexicalBytes(key)));
            }
        }
    }

    private static byte[] stringToLexicalBytes(final String value) {
        return keyNormalizers.normalize(new OCompositeKey(value), types);
    }
}
