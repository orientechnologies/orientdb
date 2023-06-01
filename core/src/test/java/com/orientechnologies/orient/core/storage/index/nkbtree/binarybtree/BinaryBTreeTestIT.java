package com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.comparator.OComparatorFactory;
import com.orientechnologies.common.io.OFileUtils;
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
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class BinaryBTreeTestIT {
    private static KeyNormalizers keyNormalizers;
    private static OType[] types;

    private OAtomicOperationsManager atomicOperationsManager;
    private BinaryBTree binaryBTree;
    private OrientDB orientDB;

    private String dbName;

    @BeforeClass
    public static void beforeClass() {
        keyNormalizers = new KeyNormalizers(Locale.ENGLISH, Collator.NO_DECOMPOSITION);
        types = new OType[] {OType.STRING};
    }

    @Before
    public void before() throws Exception {
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.setValue(4);
        OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.setValue(1024);

        final String buildDirectory =
                System.getProperty("buildDirectory", ".")
                        + File.separator
                        + BinaryBTree.class.getSimpleName();

        dbName = "binaryBTreeTest";
        final File dbDirectory = new File(buildDirectory, dbName);
        OFileUtils.deleteRecursively(dbDirectory);

        final OrientDBConfig config =
                OrientDBConfig.builder()
                        .addConfig(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE, 4)
                        .addConfig(OGlobalConfiguration.SBTREE_MAX_KEY_SIZE, 1024)
                        .build();

        orientDB = new OrientDB("plocal:" + buildDirectory, config);
        orientDB.create(dbName, ODatabaseType.PLOCAL);

        OAbstractPaginatedStorage storage;
        try (ODatabaseSession databaseDocumentTx = orientDB.open(dbName, "admin", "admin")) {
            storage =
                    (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseDocumentTx).getStorage();
        }
        binaryBTree = new BinaryBTree(1, 1024, 16, storage, "singleBTree", ".bbt");
        atomicOperationsManager = storage.getAtomicOperationsManager();
        atomicOperationsManager.executeInsideAtomicOperation(
                null, atomicOperation -> binaryBTree.create(atomicOperation));
    }

    @After
    public void afterMethod() {
        orientDB.drop(dbName);
        orientDB.close();
    }

    @Test
    public void testKeyPut() throws Exception {
        final int keysCount = 1_000_000;

        String[] lastKey = new String[1];
        for (int i = 0; i < keysCount; i++) {
            final int iteration = i;
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation -> {
                        final String key = Integer.toString(iteration);
                        binaryBTree.put(
                                atomicOperation,
                                stringToLexicalBytes(key),
                                new ORecordId(iteration % 32000, iteration));

                        if ((iteration + 1) % 100_000 == 0) {
                            System.out.printf("%d items loaded out of %d%n", iteration + 1, keysCount);
                        }

                        if (lastKey[0] == null) {
                            lastKey[0] = key;
                        } else if (key.compareTo(lastKey[0]) > 0) {
                            lastKey[0] = key;
                        }
                    });

            Assert.assertArrayEquals(stringToLexicalBytes("0"), binaryBTree.firstKey());
            Assert.assertArrayEquals(stringToLexicalBytes(lastKey[0]), binaryBTree.lastKey());
        }

        for (int i = 0; i < keysCount; i++) {
            Assert.assertEquals(
                    i + " key is absent",
                    new ORecordId(i % 32000, i),
                    binaryBTree.get(stringToLexicalBytes(Integer.toString(i))));
            if (i % 100_000 == 0) {
                System.out.printf("%d items tested out of %d%n", i, keysCount);
            }
        }

        for (int i = keysCount; i < 2 * keysCount; i++) {
            Assert.assertNull(binaryBTree.get(stringToLexicalBytes(Integer.toString(i))));
        }
    }

    @Test
    public void testKeyPutRandomUniform() throws Exception {
        final TreeMap<String, ORID> keys = new TreeMap<>();
        final long seed = System.nanoTime();
        System.out.println("testKeyPutRandomUniform seed : " + seed);
        final Random random = new Random(seed);
        final int keysCount = 1_000_000;

        while (keys.size() < keysCount) {
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation -> {
                        int val = random.nextInt(Integer.MAX_VALUE);
                        String key = Integer.toString(val);
                        binaryBTree.put(
                                atomicOperation, stringToLexicalBytes(key), new ORecordId(val % 32000, val));

                        keys.put(key, new ORecordId(val % 32000, val));

                        Assert.assertEquals(
                                binaryBTree.get(stringToLexicalBytes(key)), new ORecordId(val % 32000, val));

                        Assert.assertArrayEquals(stringToLexicalBytes(keys.firstKey()), binaryBTree.firstKey());
                        Assert.assertArrayEquals(stringToLexicalBytes(keys.lastKey()), binaryBTree.lastKey());
                    });
        }

        for (Map.Entry<String, ORID> entry : keys.entrySet()) {
            Assert.assertEquals(entry.getValue(), binaryBTree.get(stringToLexicalBytes(entry.getKey())));
        }
    }

    @Test
    public void testKeyPutRandomGaussian() throws Exception {
        final TreeMap<String, ORID> keys = new TreeMap<>();
        long seed = System.currentTimeMillis();
        System.out.println("testKeyPutRandomGaussian seed : " + seed);

        Random random = new Random(seed);
        final int keysCount = 1_000_000;

        while (keys.size() < keysCount) {
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation -> {
                        int val;
                        do {
                            val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
                        } while (val < 0);

                        final String key = Integer.toString(val);
                        binaryBTree.put(
                                atomicOperation, stringToLexicalBytes(key), new ORecordId(val % 32000, val));
                        keys.put(key, new ORecordId(val % 32000, val));
                        Assert.assertEquals(
                                new ORecordId(val % 32000, val), binaryBTree.get(stringToLexicalBytes(key)));
                    });
        }

        Assert.assertArrayEquals(stringToLexicalBytes(keys.firstKey()), binaryBTree.firstKey());
        Assert.assertArrayEquals(stringToLexicalBytes(keys.lastKey()), binaryBTree.lastKey());

        for (Map.Entry<String, ORID> entry : keys.entrySet()) {
            Assert.assertEquals(entry.getValue(), binaryBTree.get(stringToLexicalBytes(entry.getKey())));
        }
    }

    @Test
    public void testKeyDeleteRandomUniform() throws Exception {
        final int keysCount = 1_000_000;

        final TreeMap<String, ORID> keyMap = new TreeMap<>();
        for (int i = 0; i < keysCount; i++) {
            final String key = Integer.toString(i);
            final int k = i;
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation ->
                            binaryBTree.put(
                                    atomicOperation, stringToLexicalBytes(key), new ORecordId(k % 32000, k)));
            keyMap.put(key, new ORecordId(k % 32000, k));
        }

        Iterator<String> keysIterator = keyMap.keySet().iterator();
        while (keysIterator.hasNext()) {
            final String key = keysIterator.next();
            final long val = keyMap.get(key).getClusterPosition();
            if (val % 3 == 0) {
                atomicOperationsManager.executeInsideAtomicOperation(
                        null,
                        atomicOperation -> binaryBTree.remove(atomicOperation, stringToLexicalBytes(key)));
                keysIterator.remove();
            }
        }

        Assert.assertArrayEquals(stringToLexicalBytes(keyMap.firstKey()), binaryBTree.firstKey());
        Assert.assertArrayEquals(stringToLexicalBytes(keyMap.lastKey()), binaryBTree.lastKey());

        for (final Map.Entry<String, ORID> entry : keyMap.entrySet()) {
            long val = entry.getValue().getClusterPosition();
            if (val % 3 == 0) {
                Assert.assertNull(binaryBTree.get(stringToLexicalBytes(entry.getKey())));
            } else {
                Assert.assertEquals(
                        entry.getValue(), binaryBTree.get(stringToLexicalBytes(entry.getKey())));
            }
        }
    }

    @Test
    public void testKeyDeleteRandomGaussian() throws Exception {
        final TreeMap<String, ORID> keys = new TreeMap<>();

        final int keysCount = 1_000_000;
        long seed = System.currentTimeMillis();
        System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
        Random random = new Random(seed);

        while (keys.size() < keysCount) {
            int val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
            if (val < 0) {
                continue;
            }
            String key = Integer.toString(val);
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation ->
                            binaryBTree.put(
                                    atomicOperation, stringToLexicalBytes(key), new ORecordId(val % 32000, val)));
            keys.put(key, new ORecordId(val % 32000, val));

            Assert.assertEquals(
                    binaryBTree.get(stringToLexicalBytes(key)), new ORecordId(val % 32000, val));
        }

        Iterator<Map.Entry<String, ORID>> keysIterator = keys.entrySet().iterator();

        while (keysIterator.hasNext()) {
            final Map.Entry<String, ORID> entry = keysIterator.next();

            if (entry.getValue().getClusterPosition() % 3 == 0) {
                atomicOperationsManager.executeInsideAtomicOperation(
                        null,
                        atomicOperation ->
                                binaryBTree.remove(atomicOperation, stringToLexicalBytes(entry.getKey())));
                keysIterator.remove();
            }
        }

        Assert.assertArrayEquals(stringToLexicalBytes(keys.firstKey()), binaryBTree.firstKey());
        Assert.assertArrayEquals(stringToLexicalBytes(keys.lastKey()), binaryBTree.lastKey());

        for (final Map.Entry<String, ORID> entry : keys.entrySet()) {
            final int val = (int) entry.getValue().getClusterPosition();
            if (val % 3 == 0) {
                Assert.assertNull(binaryBTree.get(stringToLexicalBytes(entry.getKey())));
            } else {
                Assert.assertEquals(
                        new ORecordId(val % 32000, val), binaryBTree.get(stringToLexicalBytes(entry.getKey())));
            }
        }
    }

    @Test
    public void testKeyAddDeleteHalf() throws Exception {
        final int keysCount = 1_000_000;

        for (int i = 0; i < keysCount / 2; i++) {
            final int key = i;
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation ->
                            binaryBTree.put(
                                    atomicOperation,
                                    stringToLexicalBytes(Integer.toString(key)),
                                    new ORecordId(key % 32000, key)));
        }

        for (int iterations = 0; iterations < 4; iterations++) {
            System.out.println("testKeyAddDeleteHalf : iteration " + iterations);

            for (int i = 0; i < keysCount / 2; i++) {
                final int key = i + (iterations + 1) * keysCount / 2;
                atomicOperationsManager.executeInsideAtomicOperation(
                        null,
                        atomicOperation ->
                                binaryBTree.put(
                                        atomicOperation,
                                        stringToLexicalBytes(Integer.toString(key)),
                                        new ORecordId(key % 32000, key)));

                Assert.assertEquals(
                        binaryBTree.get(stringToLexicalBytes(Integer.toString(key))),
                        new ORecordId(key % 32000, key));
            }

            final int offset = iterations * (keysCount / 2);

            for (int i = 0; i < keysCount / 2; i++) {
                final int key = i + offset;
                atomicOperationsManager.executeInsideAtomicOperation(
                        null,
                        atomicOperation ->
                                Assert.assertEquals(
                                        binaryBTree.remove(
                                                atomicOperation, stringToLexicalBytes(Integer.toString(key))),
                                        new ORecordId(key % 32000, key)));
            }

            final int start = (iterations + 1) * (keysCount / 2);
            for (int i = 0; i < (iterations + 2) * keysCount / 2; i++) {
                if (i < start) {
                    Assert.assertNull(binaryBTree.get(stringToLexicalBytes(Integer.toString(i))));
                } else {
                    Assert.assertEquals(
                            new ORecordId(i % 32000, i),
                            binaryBTree.get(stringToLexicalBytes(Integer.toString(i))));
                }
            }

            binaryBTree.assertFreePages();
        }
    }

    @Test
    public void testKeyDelete() throws Exception {
        final int keysCount = 1_000_000;

        for (int i = 0; i < keysCount; i++) {
            final int k = i;
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation ->
                            binaryBTree.put(
                                    atomicOperation,
                                    stringToLexicalBytes(Integer.toString(k)),
                                    new ORecordId(k % 32000, k)));
        }

        for (int i = 0; i < keysCount; i++) {
            final int iteration = i;

            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation -> {
                        if (iteration % 3 == 0) {
                            Assert.assertEquals(
                                    binaryBTree.remove(
                                            atomicOperation, stringToLexicalBytes(Integer.toString(iteration))),
                                    new ORecordId(iteration % 32000, iteration));
                        }
                    });
        }

        for (int i = 0; i < keysCount; i++) {
            if (i % 3 == 0) {
                Assert.assertNull(binaryBTree.get(stringToLexicalBytes(Integer.toString(i))));
            } else {
                Assert.assertEquals(
                        binaryBTree.get(stringToLexicalBytes(Integer.toString(i))),
                        new ORecordId(i % 32000, i));
            }
        }
    }

    @Test
    public void testKeyAddDelete() throws Exception {
        final int keysCount = 1_000_000;

        for (int i = 0; i < keysCount; i++) {
            final int key = i;
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation ->
                            binaryBTree.put(
                                    atomicOperation,
                                    stringToLexicalBytes(Integer.toString(key)),
                                    new ORecordId(key % 32000, key)));

            Assert.assertEquals(
                    binaryBTree.get(stringToLexicalBytes(Integer.toString(i))), new ORecordId(i % 32000, i));
        }
        final int txInterval = 100;

        for (int i = 0; i < keysCount / txInterval; i++) {
            final int iterationsCounter = i;

            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation -> {
                        for (int j = 0; j < txInterval; j++) {
                            final int key = (iterationsCounter * txInterval + j);

                            if (key % 3 == 0) {
                                Assert.assertEquals(
                                        binaryBTree.remove(
                                                atomicOperation, stringToLexicalBytes(Integer.toString(key))),
                                        new ORecordId(key % 32000, key));
                            }

                            if (key % 2 == 0) {
                                binaryBTree.put(
                                        atomicOperation,
                                        stringToLexicalBytes(Integer.toString(keysCount + key)),
                                        new ORecordId((keysCount + key) % 32000, keysCount + key));
                            }
                        }
                    });
        }

        for (int i = 0; i < keysCount; i++) {
            if (i % 3 == 0) {
                Assert.assertNull(binaryBTree.get(stringToLexicalBytes(Integer.toString(i))));
            } else {
                Assert.assertEquals(
                        binaryBTree.get(stringToLexicalBytes(Integer.toString(i))),
                        new ORecordId(i % 32000, i));
            }

            if (i % 2 == 0) {
                Assert.assertEquals(
                        binaryBTree.get(stringToLexicalBytes(Integer.toString(keysCount + i))),
                        new ORecordId((keysCount + i) % 32000, keysCount + i));
            }
        }
    }

    @Test
    public void testKeyAddDeleteAll() throws Exception {
        for (int iterations = 0; iterations < 4; iterations++) {
            System.out.println("testKeyAddDeleteAll : iteration " + iterations);

            final int keysCount = 1_000_000;

            for (int i = 0; i < keysCount; i++) {
                final int key = i;
                atomicOperationsManager.executeInsideAtomicOperation(
                        null,
                        atomicOperation ->
                                binaryBTree.put(
                                        atomicOperation,
                                        stringToLexicalBytes(Integer.toString(key)),
                                        new ORecordId(key % 32000, key)));
            }

            for (int i = 0; i < keysCount; i++) {
                final int key = i;
                atomicOperationsManager.executeInsideAtomicOperation(
                        null,
                        atomicOperation -> {
                            Assert.assertEquals(
                                    binaryBTree.remove(atomicOperation, stringToLexicalBytes(Integer.toString(key))),
                                    new ORecordId(key % 32000, key));

                            if (key > 0 && key % 100_000 == 0) {
                                for (int keyToVerify = 0; keyToVerify < keysCount; keyToVerify++) {
                                    if (keyToVerify > key) {
                                        Assert.assertEquals(
                                                new ORecordId(keyToVerify % 32000, keyToVerify),
                                                binaryBTree.get(stringToLexicalBytes(Integer.toString(keyToVerify))));
                                    } else {
                                        Assert.assertNull(
                                                binaryBTree.get(stringToLexicalBytes(Integer.toString(keyToVerify))));
                                    }
                                }
                            }
                        });
            }
            for (int i = 0; i < keysCount; i++) {
                Assert.assertNull(binaryBTree.get(stringToLexicalBytes(Integer.toString(i))));
            }

            binaryBTree.assertFreePages();
        }
    }

    @Test
    public void testRandomOperations() throws IOException {
        final int maximumKeys = 1_000_000;
        final int operations = 10 * maximumKeys;

        final TreeMap<byte[], ORID> keyMap =
                new TreeMap<>(OComparatorFactory.INSTANCE.getComparator(byte[].class));

        final long seed = System.nanoTime();
        final Random random = new Random(seed);

        System.out.println("testRandomOperations : seed " + seed);
        boolean growInSize = true;

        for (int i = 0; i < operations; i++) {
            final Operation operation;

            if (!keyMap.isEmpty()) {
                if (keyMap.size() >= maximumKeys) {
                    operation = Operation.DELETE;
                    growInSize = false;
                } else if (growInSize) {
                    if (keyMap.size() > 0.8 * maximumKeys) {
                        growInSize = false;
                    }

                    if (random.nextDouble() < 0.8) {
                        operation = Operation.INSERT;
                    } else {
                        operation = Operation.DELETE;
                    }
                } else {
                    if (random.nextDouble() < 0.8) {
                        operation = Operation.DELETE;
                    } else {
                        operation = Operation.INSERT;
                    }
                }
            } else {
                operation = Operation.INSERT;
                growInSize = true;
            }

            final int keySize = random.nextInt(10) + 5;
            final byte[] key = new byte[keySize];
            random.nextBytes(key);

            if (operation == Operation.INSERT) {
                final ORID value = new ORecordId(i % 32000, i);

                keyMap.put(key, value);
                atomicOperationsManager.executeInsideAtomicOperation(
                        null, atomicOperation -> binaryBTree.put(atomicOperation, key, value));
            } else {
                final int deletionKeySize = random.nextInt(10) + 5;
                final byte[] deletionCandidate = new byte[deletionKeySize];
                random.nextBytes(deletionCandidate);

                byte[] deletionKey = keyMap.floorKey(deletionCandidate);
                if (deletionKey == null) {
                    deletionKey = keyMap.lastKey();
                }

                final ORID expectedRemovedValue = keyMap.remove(deletionKey);

                final byte[] dKey = deletionKey;
                final ORID removedValue =
                        atomicOperationsManager.calculateInsideAtomicOperation(
                                null, atomicOperation -> binaryBTree.remove(atomicOperation, dKey));
                Assert.assertEquals(expectedRemovedValue, removedValue);
            }

            if ((i + 1) % 100_000 == 0) {
                System.out.printf("%,d operations are processed out of %,d%n", i + 1, operations);
            }
        }

        System.out.println("Checking tree consistency");
        for (final Map.Entry<byte[], ORID> entry : keyMap.entrySet()) {
            final byte[] key = entry.getKey();
            final ORID expectedValue = entry.getValue();

            final ORID value = binaryBTree.get(key);
            Assert.assertEquals(expectedValue, value);
        }

        binaryBTree.assertFreePages();

        System.out.println("Remove all keys");

        final int mapSize = keyMap.size();
        int counter = 0;
        for (final byte[] key : keyMap.keySet()) {
            atomicOperationsManager.executeInsideAtomicOperation(
                    null, atomicOperation -> binaryBTree.remove(atomicOperation, key));
            counter++;
            if (counter % 100_000 == 0) {
                System.out.printf("%,d keys are removed out ouf %,d%n", counter, mapSize);
            }
        }

        binaryBTree.assertFreePages();
    }

    @Test
    public void testIterateEntriesMajor() throws Exception {
        final int keysCount = 1_000_000;

        final Collator collator = Collator.getInstance(Locale.ENGLISH);
        collator.setDecomposition(Collator.NO_DECOMPOSITION);

        NavigableMap<String, ORID> keyValues = new TreeMap<>(collator::compare);

        final long seed = System.nanoTime();

        System.out.println("testIterateEntriesMajor: " + seed);
        final Random random = new Random(seed);

        while (keyValues.size() < keysCount) {
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation -> {
                        int val = random.nextInt(Integer.MAX_VALUE);
                        String key = Integer.toString(val);

                        binaryBTree.put(
                                atomicOperation, stringToLexicalBytes(key), new ORecordId(val % 32000, val));
                        keyValues.put(key, new ORecordId(val % 32000, val));
                    });
        }

        assertIterateMajorEntries(keyValues, random, true, true);
        assertIterateMajorEntries(keyValues, random, false, true);

        assertIterateMajorEntries(keyValues, random, true, false);
        assertIterateMajorEntries(keyValues, random, false, false);

        Assert.assertArrayEquals(binaryBTree.firstKey(), stringToLexicalBytes(keyValues.firstKey()));
        Assert.assertArrayEquals(binaryBTree.lastKey(), stringToLexicalBytes(keyValues.lastKey()));
    }

    @Test
    public void testIterateEntriesMinor() throws Exception {
        final int keysCount = 1_000_000;

        final Collator collator = Collator.getInstance(Locale.ENGLISH);
        collator.setDecomposition(Collator.NO_DECOMPOSITION);

        NavigableMap<String, ORID> keyValues = new TreeMap<>(collator::compare);

        final long seed = System.nanoTime();

        System.out.println("testIterateEntriesMinor: " + seed);
        final Random random = new Random(seed);

        while (keyValues.size() < keysCount) {
            atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation -> {
                        int val = random.nextInt(Integer.MAX_VALUE);
                        String key = Integer.toString(val);

                        binaryBTree.put(
                                atomicOperation, stringToLexicalBytes(key), new ORecordId(val % 32000, val));

                        keyValues.put(key, new ORecordId(val % 32000, val));
                    });
        }

        assertIterateMinorEntries(keyValues, random, true, true);
        assertIterateMinorEntries(keyValues, random, false, true);

        assertIterateMinorEntries(keyValues, random, true, false);
        assertIterateMinorEntries(keyValues, random, false, false);

        Assert.assertArrayEquals(stringToLexicalBytes(keyValues.firstKey()), binaryBTree.firstKey());
        Assert.assertArrayEquals(stringToLexicalBytes(keyValues.lastKey()), binaryBTree.lastKey());
    }

    @Test
    public void testIterateEntriesBetween() throws Exception {
        final int keysCount = 1_000_000;
        final Collator collator = Collator.getInstance(Locale.ENGLISH);
        collator.setDecomposition(Collator.NO_DECOMPOSITION);

        NavigableMap<String, ORID> keyValues = new TreeMap<>(collator::compare);

        final long seed = System.nanoTime();
        System.out.println("testIterateEntriesBetween seed: " + seed);
        final Random random = new Random(seed);

        while (keyValues.size() < keysCount) {
            for (int n = 0; n < 2; n++) {
                atomicOperationsManager.executeInsideAtomicOperation(
                        null,
                        atomicOperation -> {
                            int val = random.nextInt(Integer.MAX_VALUE);
                            String key = Integer.toString(val);

                            binaryBTree.put(
                                    atomicOperation, stringToLexicalBytes(key), new ORecordId(val % 32000, val));
                            keyValues.put(key, new ORecordId(val % 32000, val));
                        });
            }
        }
        assertIterateBetweenEntries(keyValues, random, collator, true, true, true);
        assertIterateBetweenEntries(keyValues, random, collator, true, false, true);
        assertIterateBetweenEntries(keyValues, random, collator, false, true, true);
        assertIterateBetweenEntries(keyValues, random, collator, false, false, true);

        assertIterateBetweenEntries(keyValues, random, collator, true, true, false);
        assertIterateBetweenEntries(keyValues, random, collator, true, false, false);
        assertIterateBetweenEntries(keyValues, random, collator, false, true, false);
        assertIterateBetweenEntries(keyValues, random, collator, false, false, false);

        Assert.assertArrayEquals(stringToLexicalBytes(keyValues.firstKey()), binaryBTree.firstKey());
        Assert.assertArrayEquals(stringToLexicalBytes(keyValues.lastKey()), binaryBTree.lastKey());
    }

    private void assertIterateBetweenEntries(
            NavigableMap<String, ORID> keyValues,
            Random random,
            Collator collator,
            boolean fromInclusive,
            boolean toInclusive,
            boolean ascSortOrder) {
        String[] keys = new String[keyValues.size()];
        int index = 0;

        for (String key : keyValues.keySet()) {
            keys[index] = key;
            index++;
        }

        for (int i = 0; i < 100; i++) {
            int fromKeyIndex = random.nextInt(keys.length);
            int toKeyIndex = random.nextInt(keys.length);

            if (fromKeyIndex > toKeyIndex) {
                toKeyIndex = fromKeyIndex;
            }

            String fromKey = keys[fromKeyIndex];
            String toKey = keys[toKeyIndex];

            if (random.nextBoolean()) {
                fromKey =
                        fromKey.substring(0, fromKey.length() - 1)
                                + (char) (fromKey.charAt(fromKey.length() - 1) - 1);
            }

            if (random.nextBoolean()) {
                toKey =
                        toKey.substring(0, toKey.length() - 1) + (char) (toKey.charAt(toKey.length() - 1) + 1);
            }

            if (collator.compare(fromKey, toKey) > 0) {
                fromKey = toKey;
            }

            final Iterator<ORawPair<byte[], ORID>> indexIterator;
            try (Stream<ORawPair<byte[], ORID>> stream =
                         binaryBTree.iterateEntriesBetween(
                                 stringToLexicalBytes(fromKey),
                                 fromInclusive,
                                 stringToLexicalBytes(toKey),
                                 toInclusive,
                                 ascSortOrder)) {
                indexIterator = stream.iterator();

                Iterator<Map.Entry<String, ORID>> iterator;
                if (ascSortOrder) {
                    iterator =
                            keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).entrySet().iterator();
                } else {
                    iterator =
                            keyValues
                                    .subMap(fromKey, fromInclusive, toKey, toInclusive)
                                    .descendingMap()
                                    .entrySet()
                                    .iterator();
                }

                while (iterator.hasNext()) {
                    final ORawPair<byte[], ORID> indexEntry = indexIterator.next();
                    Assert.assertNotNull(indexEntry);

                    Map.Entry<String, ORID> mapEntry = iterator.next();
                    Assert.assertArrayEquals(stringToLexicalBytes(mapEntry.getKey()), indexEntry.first);
                    Assert.assertEquals(mapEntry.getValue(), indexEntry.second);
                }
                //noinspection ConstantConditions
                Assert.assertFalse(iterator.hasNext());
                Assert.assertFalse(indexIterator.hasNext());
            }
        }
    }

    private void assertIterateMinorEntries(
            NavigableMap<String, ORID> keyValues,
            Random random,
            boolean keyInclusive,
            boolean ascSortOrder) {
        String[] keys = new String[keyValues.size()];
        int index = 0;

        for (String key : keyValues.keySet()) {
            keys[index] = key;
            index++;
        }

        for (int i = 0; i < 100; i++) {
            int toKeyIndex = random.nextInt(keys.length);
            String toKey = keys[toKeyIndex];
            if (random.nextBoolean()) {
                toKey =
                        toKey.substring(0, toKey.length() - 1) + (char) (toKey.charAt(toKey.length() - 1) + 1);
            }

            final Iterator<ORawPair<byte[], ORID>> indexIterator;
            try (Stream<ORawPair<byte[], ORID>> stream =
                         binaryBTree.iterateEntriesMinor(
                                 stringToLexicalBytes(toKey), keyInclusive, ascSortOrder)) {
                indexIterator = stream.iterator();

                Iterator<Map.Entry<String, ORID>> iterator;
                if (ascSortOrder) {
                    iterator = keyValues.headMap(toKey, keyInclusive).entrySet().iterator();
                } else {
                    iterator = keyValues.headMap(toKey, keyInclusive).descendingMap().entrySet().iterator();
                }

                while (iterator.hasNext()) {
                    ORawPair<byte[], ORID> indexEntry = indexIterator.next();
                    Map.Entry<String, ORID> entry = iterator.next();

                    Assert.assertArrayEquals(stringToLexicalBytes(entry.getKey()), indexEntry.first);
                    Assert.assertEquals(indexEntry.second, entry.getValue());
                }

                //noinspection ConstantConditions
                Assert.assertFalse(iterator.hasNext());
                Assert.assertFalse(indexIterator.hasNext());
            }
        }
    }

    private void assertIterateMajorEntries(
            NavigableMap<String, ORID> keyValues,
            Random random,
            boolean keyInclusive,
            boolean ascSortOrder) {
        String[] keys = new String[keyValues.size()];
        int index = 0;

        for (String key : keyValues.keySet()) {
            keys[index] = key;
            index++;
        }

        for (int i = 0; i < 100; i++) {
            final int fromKeyIndex = random.nextInt(keys.length);
            String fromKey = keys[fromKeyIndex];

            if (random.nextBoolean()) {
                fromKey =
                        fromKey.substring(0, fromKey.length() - 1)
                                + (char) (fromKey.charAt(fromKey.length() - 1) - 1);
            }

            final Iterator<ORawPair<byte[], ORID>> indexIterator;
            try (Stream<ORawPair<byte[], ORID>> stream =
                         binaryBTree.iterateEntriesMajor(
                                 stringToLexicalBytes(fromKey), keyInclusive, ascSortOrder)) {
                indexIterator = stream.iterator();

                Iterator<Map.Entry<String, ORID>> iterator;
                if (ascSortOrder) {
                    iterator = keyValues.tailMap(fromKey, keyInclusive).entrySet().iterator();
                } else {
                    iterator = keyValues.tailMap(fromKey, keyInclusive).descendingMap().entrySet().iterator();
                }

                while (iterator.hasNext()) {
                    final ORawPair<byte[], ORID> indexEntry = indexIterator.next();
                    final Map.Entry<String, ORID> entry = iterator.next();

                    Assert.assertArrayEquals(indexEntry.first, stringToLexicalBytes(entry.getKey()));
                    Assert.assertEquals(indexEntry.second, entry.getValue());
                }

                //noinspection ConstantConditions
                Assert.assertFalse(iterator.hasNext());
                Assert.assertFalse(indexIterator.hasNext());
            }
        }
    }

    private static byte[] stringToLexicalBytes(final String value) {
        return keyNormalizers.normalize(new OCompositeKey(value), types);
    }

    enum Operation {
        INSERT,
        DELETE
    }
}