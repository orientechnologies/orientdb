package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

public class PrefixBTreeTestIT {
  private   OPrefixBTree<OIdentifiable> prefixTree;
  protected ODatabaseSession            databaseDocumentTx;
  protected String                      buildDirectory;
  protected OrientDB                    orientDB;

  private String dbName;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".") + File.separator + PrefixBTreeTestIT.class.getSimpleName();

    dbName = "localPrefixBTreeTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    databaseDocumentTx = orientDB.open(dbName, "admin", "admin");

    prefixTree = new OPrefixBTree<>("prefixBTree", ".pbt", ".npt",
        (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());
    prefixTree.create(OUTF8Serializer.INSTANCE, OLinkSerializer.INSTANCE, false, null);
  }

  @After
  public void afterMethod() throws Exception {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  public void testKeyPut() {
    final int keysCount = 500_000;

    String lastKey = null;

    for (int i = 0; i < keysCount; i++) {
      final String key = Integer.toString(i);

      prefixTree.put(key, new ORecordId(i % 32000, i));
      if (i % 100_000 == 0) {
        System.out.printf("%d items loaded out of %d\n", i, keysCount);
      }

      if (lastKey == null) {
        lastKey = key;
      } else if (key.compareTo(lastKey) > 0) {
        lastKey = key;
      }

      Assert.assertEquals("0", prefixTree.firstKey());
      Assert.assertEquals(lastKey, prefixTree.lastKey());
    }

    for (int i = 0; i < keysCount; i++) {
      Assert.assertEquals(i + " key is absent", prefixTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      if (i % 100_000 == 0) {
        System.out.printf("%d items tested out of %d\n", i, keysCount);
      }
    }

    for (int i = keysCount; i < 2 * keysCount; i++) {
      Assert.assertNull(prefixTree.get(Integer.toString(i)));
    }
  }

  @Test
  public void testKeyPutRandomUniform() {
    final NavigableSet<String> keys = new TreeSet<>();
    final Random random = new Random();
    final int keysCount = 500_000;

    while (keys.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      prefixTree.put(key, new ORecordId(val % 32000, val));
      keys.add(key);

      Assert.assertEquals(prefixTree.get(key), new ORecordId(val % 32000, val));
    }

    Assert.assertEquals(prefixTree.firstKey(), keys.first());
    Assert.assertEquals(prefixTree.lastKey(), keys.last());

    for (String key : keys) {
      final int val = Integer.parseInt(key);
      Assert.assertEquals(prefixTree.get(key), new ORecordId(val % 32000, val));
    }
  }

  @Test
  public void testKeyPutRandomGaussian() {
    NavigableSet<String> keys = new TreeSet<>();
    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);
    final int keysCount = 500_000;

    while (keys.size() < keysCount) {
      int val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (val < 0)
        continue;

      String key = Integer.toString(val);
      prefixTree.put(key, new ORecordId(val % 32000, val));
      keys.add(key);

      Assert.assertEquals(prefixTree.get(key), new ORecordId(val % 32000, val));
    }

    Assert.assertEquals(prefixTree.firstKey(), keys.first());
    Assert.assertEquals(prefixTree.lastKey(), keys.last());

    for (String key : keys) {
      int val = Integer.parseInt(key);
      Assert.assertEquals(prefixTree.get(key), new ORecordId(val % 32000, val));
    }
  }

  @Test
  public void testKeyDeleteRandomUniform() {
    final int keysCount = 500_000;

    NavigableSet<String> keys = new TreeSet<>();
    for (int i = 0; i < keysCount; i++) {
      String key = Integer.toString(i);
      prefixTree.put(key, new ORecordId(i % 32000, i));
      keys.add(key);
    }

    Iterator<String> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      String key = keysIterator.next();
      if (Integer.parseInt(key) % 3 == 0) {
        prefixTree.remove(key);
        keysIterator.remove();
      }
    }

    Assert.assertEquals(prefixTree.firstKey(), keys.first());
    Assert.assertEquals(prefixTree.lastKey(), keys.last());

    for (String key : keys) {
      int val = Integer.parseInt(key);
      if (val % 3 == 0) {
        Assert.assertNull(prefixTree.get(key));
      } else {
        Assert.assertEquals(prefixTree.get(key), new ORecordId(val % 32000, val));
      }
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() {
    NavigableSet<String> keys = new TreeSet<>();
    final int keysCount = 500_000;

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < keysCount) {
      int val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (val < 0)
        continue;

      String key = Integer.toString(val);
      prefixTree.put(key, new ORecordId(val % 32000, val));
      keys.add(key);

      Assert.assertEquals(prefixTree.get(key), new ORecordId(val % 32000, val));
    }

    Iterator<String> keysIterator = keys.iterator();

    while (keysIterator.hasNext()) {
      String key = keysIterator.next();

      if (Integer.parseInt(key) % 3 == 0) {
        prefixTree.remove(key);
        keysIterator.remove();
      }
    }

    Assert.assertEquals(prefixTree.firstKey(), keys.first());
    Assert.assertEquals(prefixTree.lastKey(), keys.last());

    for (String key : keys) {
      int val = Integer.parseInt(key);
      if (val % 3 == 0) {
        Assert.assertNull(prefixTree.get(key));
      } else {
        Assert.assertEquals(prefixTree.get(key), new ORecordId(val % 32000, val));
      }
    }
  }

  @Test
  public void testKeyDelete() {
    final int keysCount = 500_000;

    for (int i = 0; i < keysCount; i++) {
      prefixTree.put(Integer.toString(i), new ORecordId(i % 32000, i));
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertEquals(prefixTree.remove(Integer.toString(i)), new ORecordId(i % 32000, i));
      }
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(prefixTree.get(Integer.toString(i)));
      } else {
        Assert.assertEquals(prefixTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      }
    }
  }

  @Test
  public void testKeyAddDelete() {
    final int keysCount = 500_000;

    for (int i = 0; i < keysCount; i++) {
      prefixTree.put(Integer.toString(i), new ORecordId(i % 32000, i));

      Assert.assertEquals(prefixTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertEquals(prefixTree.remove(Integer.toString(i)), new ORecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        prefixTree.put(Integer.toString(keysCount + i), new ORecordId((keysCount + i) % 32000, keysCount + i));
      }

    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(prefixTree.get(Integer.toString(i)));
      } else {
        Assert.assertEquals(prefixTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        Assert.assertEquals(prefixTree.get(Integer.toString(keysCount + i)), new ORecordId((keysCount + i) % 32000, keysCount + i));
      }
    }
  }

  @Test
  public void testKeyCursor() {
    final int keysCount = 500_000;

    NavigableMap<String, ORID> keyValues = new TreeMap<>();
    final long seed = System.nanoTime();

    System.out.println("testKeyCursor: " + seed);
    Random random = new Random(seed);

    while (keyValues.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      prefixTree.put(key, new ORecordId(val % 32000, val));
      keyValues.put(key, new ORecordId(val % 32000, val));
    }

    Assert.assertEquals(prefixTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(prefixTree.lastKey(), keyValues.lastKey());

    final OPrefixBTree.OSBTreeKeyCursor<String> cursor = prefixTree.keyCursor();

    for (String entryKey : keyValues.keySet()) {
      final String indexKey = cursor.next(-1);
      Assert.assertEquals(entryKey, indexKey);
    }
  }

  @Test
  public void testIterateEntriesMajor() {
    final int keysCount = 500_000;

    NavigableMap<String, ORID> keyValues = new TreeMap<>();
    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMajor: " + seed);
    Random random = new Random(seed);

    while (keyValues.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      prefixTree.put(key, new ORecordId(val % 32000, val));
      keyValues.put(key, new ORecordId(val % 32000, val));
    }

    assertIterateMajorEntries(keyValues, random, true, true);
    assertIterateMajorEntries(keyValues, random, false, true);

    assertIterateMajorEntries(keyValues, random, true, false);
    assertIterateMajorEntries(keyValues, random, false, false);

    Assert.assertEquals(prefixTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(prefixTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesMinor() {
    final int keysCount = 500_000;
    NavigableMap<String, ORID> keyValues = new TreeMap<>();

    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMinor: " + seed);
    Random random = new Random(seed);

    while (keyValues.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      prefixTree.put(key, new ORecordId(val % 32000, val));
      keyValues.put(key, new ORecordId(val % 32000, val));
    }

    assertIterateMinorEntries(keyValues, random, true, true);
    assertIterateMinorEntries(keyValues, random, false, true);

    assertIterateMinorEntries(keyValues, random, true, false);
    assertIterateMinorEntries(keyValues, random, false, false);

    Assert.assertEquals(prefixTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(prefixTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesBetween() {
    final int keysCount = 500_000;
    NavigableMap<String, ORID> keyValues = new TreeMap<>();
    Random random = new Random();

    while (keyValues.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      prefixTree.put(key, new ORecordId(val % 32000, val));
      keyValues.put(key, new ORecordId(val % 32000, val));
    }

    assertIterateBetweenEntries(keyValues, random, true, true, true);
    assertIterateBetweenEntries(keyValues, random, true, false, true);
    assertIterateBetweenEntries(keyValues, random, false, true, true);
    assertIterateBetweenEntries(keyValues, random, false, false, true);

    assertIterateBetweenEntries(keyValues, random, true, true, false);
    assertIterateBetweenEntries(keyValues, random, true, false, false);
    assertIterateBetweenEntries(keyValues, random, false, true, false);
    assertIterateBetweenEntries(keyValues, random, false, false, false);

    Assert.assertEquals(prefixTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(prefixTree.lastKey(), keyValues.lastKey());
  }

  private void assertIterateMajorEntries(NavigableMap<String, ORID> keyValues, Random random, boolean keyInclusive,
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
        fromKey = fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      final OPrefixBTree.OSBTreeCursor<String, OIdentifiable> cursor = prefixTree
          .iterateEntriesMajor(fromKey, keyInclusive, ascSortOrder);

      Iterator<Map.Entry<String, ORID>> iterator;
      if (ascSortOrder)
        iterator = keyValues.tailMap(fromKey, keyInclusive).entrySet().iterator();
      else
        iterator = keyValues.descendingMap().subMap(keyValues.lastKey(), true, fromKey, keyInclusive).entrySet().iterator();

      while (iterator.hasNext()) {
        final Map.Entry<String, OIdentifiable> indexEntry = cursor.next(-1);
        final Map.Entry<String, ORID> entry = iterator.next();

        Assert.assertEquals(indexEntry.getKey(), entry.getKey());
        Assert.assertEquals(indexEntry.getValue(), entry.getValue());
      }

      Assert.assertFalse(iterator.hasNext());
      Assert.assertNull(cursor.next(-1));
    }
  }

  private void assertIterateMinorEntries(NavigableMap<String, ORID> keyValues, Random random, boolean keyInclusive,
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
        toKey = toKey.substring(0, toKey.length() - 2) + (toKey.charAt(toKey.length() - 1) + 1);
      }

      final OPrefixBTree.OSBTreeCursor<String, OIdentifiable> cursor = prefixTree
          .iterateEntriesMinor(toKey, keyInclusive, ascSortOrder);

      Iterator<Map.Entry<String, ORID>> iterator;
      if (ascSortOrder) {
        iterator = keyValues.headMap(toKey, keyInclusive).entrySet().iterator();
      } else {
        iterator = keyValues.headMap(toKey, keyInclusive).descendingMap().entrySet().iterator();
      }

      while (iterator.hasNext()) {
        Map.Entry<String, OIdentifiable> indexEntry = cursor.next(-1);
        Map.Entry<String, ORID> entry = iterator.next();

        Assert.assertEquals(indexEntry.getKey(), entry.getKey());
        Assert.assertEquals(indexEntry.getValue(), entry.getValue());
      }

      Assert.assertFalse(iterator.hasNext());
      Assert.assertNull(cursor.next(-1));
    }
  }

  private void assertIterateBetweenEntries(NavigableMap<String, ORID> keyValues, Random random, boolean fromInclusive,
      boolean toInclusive, boolean ascSortOrder) {
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
        fromKey = fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      if (random.nextBoolean()) {
        toKey = toKey.substring(0, toKey.length() - 2) + (toKey.charAt(toKey.length() - 1) + 1);
      }

      if (fromKey.compareTo(toKey) > 0) {
        fromKey = toKey;
      }

      OPrefixBTree.OSBTreeCursor<String, OIdentifiable> cursor = prefixTree
          .iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascSortOrder);

      Iterator<Map.Entry<String, ORID>> iterator;
      if (ascSortOrder) {
        iterator = keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).entrySet().iterator();
      } else {
        iterator = keyValues.descendingMap().subMap(toKey, toInclusive, fromKey, fromInclusive).entrySet().iterator();
      }

      while (iterator.hasNext()) {
        Map.Entry<String, OIdentifiable> indexEntry = cursor.next(-1);
        Assert.assertNotNull(indexEntry);

        Map.Entry<String, ORID> mapEntry = iterator.next();
        Assert.assertEquals(indexEntry.getKey(), mapEntry.getKey());
        Assert.assertEquals(indexEntry.getValue(), mapEntry.getValue());
      }
      Assert.assertFalse(iterator.hasNext());
      Assert.assertNull(cursor.next(-1));
    }
  }
}
