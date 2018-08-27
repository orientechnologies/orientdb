package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

public class PrefixBTreeTestIT {
  private static final int                         KEYS_COUNT = 1_000_000;
  private              OPrefixBTree<OIdentifiable> prefixTree;
  protected            ODatabaseSession            databaseDocumentTx;
  protected            String                      buildDirectory;
  protected            OrientDB                    orientDB;

  private String dbName;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".") + File.separator + SBTreeTestIT.class.getSimpleName();

    dbName = "localPrefixBTreeTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    databaseDocumentTx = orientDB.open(dbName, "admin", "admin");

    prefixTree = new OPrefixBTree<>("prefixBTree", ".sbt", ".nbt",
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
    String lastKey = null;

    for (int i = 0; i < KEYS_COUNT; i++) {
      final String key = Integer.toString(i);

      prefixTree.put(key, new ORecordId(i % 32000, i));
      if (i % 100_000 == 0) {
        System.out.printf("%d items loaded out of %d\n", i, KEYS_COUNT);
      }

      if (lastKey == null) {
        lastKey = key;
      } else if (key.compareTo(lastKey) > 0) {
        lastKey = key;
      }

      Assert.assertEquals("0", prefixTree.firstKey());
      Assert.assertEquals(lastKey, prefixTree.lastKey());
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      Assert.assertEquals(i + " key is absent", prefixTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      if (i % 100_000 == 0) {
        System.out.printf("%d items tested out of %d\n", i, KEYS_COUNT);
      }
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertNull(prefixTree.get(Integer.toString(i)));
    }
  }

  @Test
  public void testKeyPutRandomUniform() {
    final NavigableSet<String> keys = new TreeSet<>();
    final Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
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
  public void testKeyPutRandomGaussian() throws Exception {
    NavigableSet<String> keys = new TreeSet<>();
    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);

    while (keys.size() < KEYS_COUNT) {
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
    NavigableSet<String> keys = new TreeSet<>();
    for (int i = 0; i < KEYS_COUNT; i++) {
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
  public void testKeyDeleteRandomGaussian() throws Exception {
    NavigableSet<String> keys = new TreeSet<>();

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < KEYS_COUNT) {
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
    for (int i = 0; i < KEYS_COUNT; i++) {
      prefixTree.put(Integer.toString(i), new ORecordId(i % 32000, i));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        Assert.assertEquals(prefixTree.remove(Integer.toString(i)), new ORecordId(i % 32000, i));
      }
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(prefixTree.get(Integer.toString(i)));
      } else {
        Assert.assertEquals(prefixTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      }
    }
  }

  @Test
  public void testKeyAddDelete() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      prefixTree.put(Integer.toString(i), new ORecordId(i % 32000, i));

      Assert.assertEquals(prefixTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        Assert.assertEquals(prefixTree.remove(Integer.toString(i)), new ORecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        prefixTree.put(Integer.toString(KEYS_COUNT + i), new ORecordId((KEYS_COUNT + i) % 32000, KEYS_COUNT + i));
      }

    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(prefixTree.get(Integer.toString(i)));
      } else {
        Assert.assertEquals(prefixTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        Assert.assertEquals(prefixTree.get(Integer.toString(KEYS_COUNT + i)),
            new ORecordId((KEYS_COUNT + i) % 32000, KEYS_COUNT + i));
      }
    }
  }
}
