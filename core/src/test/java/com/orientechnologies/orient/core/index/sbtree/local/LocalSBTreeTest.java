package com.orientechnologies.orient.core.index.sbtree.local;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.index.sbtree.OLocalSBTree;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Andrey Lomakin
 * @since 12.08.13
 */
@Test
public class LocalSBTreeTest {
  private static final int      KEYS_COUNT = 500000;

  private ODatabaseDocumentTx   databaseDocumentTx;

  private OLocalSBTree<Integer> localSBTree;
  private String                buildDirectory;

  @BeforeClass
  public void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/localSBTreeTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new OMurmurHash3HashFunction<Integer>();
    murmurHash3HashFunction.setValueSerializer(OIntegerSerializer.INSTANCE);

    localSBTree = new OLocalSBTree<Integer>(".sbt");
    localSBTree.create("localSBTree", OIntegerSerializer.INSTANCE, (OStorageLocal) databaseDocumentTx.getStorage());
  }

  @AfterMethod
  public void afterMethod() {
    localSBTree.clear();
  }

  @AfterClass
  public void afterClass() throws Exception {
    localSBTree.clear();
    localSBTree.delete();
    databaseDocumentTx.drop();
  }

  public void testKeyPut() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localSBTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++)
      Assert.assertEquals(localSBTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)), i
          + " key is absent");

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++)
      Assert.assertNull(localSBTree.get(i));

  }

  public void testKeyPutRandomUniform() {
    final Set<Integer> keys = new HashSet<Integer>();
    final MersenneTwisterFast random = new MersenneTwisterFast();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);
      localSBTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keys.add(key);

      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    for (int key : keys)
      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
  }

  public void testKeyPutRandomGaussian() {
    Set<Integer> keys = new HashSet<Integer>();
    long seed = 1376477211861L;

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    MersenneTwisterFast random = new MersenneTwisterFast(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      localSBTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keys.add(key);

      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    for (int key : keys)
      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
  }

  public void testKeyDeleteRandomUniform() {
    HashSet<Integer> keys = new HashSet<Integer>();
    for (int i = 0; i < KEYS_COUNT; i++) {
      localSBTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
      keys.add(i);
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localSBTree.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localSBTree.get(key));
      } else {
        Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      }
    }
  }

  public void testKeyDeleteRandomGaussian() {
    HashSet<Integer> keys = new HashSet<Integer>();

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    MersenneTwisterFast random = new MersenneTwisterFast(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      localSBTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keys.add(key);

      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localSBTree.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localSBTree.get(key));
      } else {
        Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      }
    }
  }

  public void testKeyDelete() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localSBTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localSBTree.remove(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localSBTree.get(i));
      else
        Assert.assertEquals(localSBTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }
  }

  public void testKeyAddDelete() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localSBTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

      Assert.assertEquals(localSBTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localSBTree.remove(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

      if (i % 2 == 0)
        localSBTree.put(KEYS_COUNT + i,
            new ORecordId((KEYS_COUNT + i) % 32000, OClusterPositionFactory.INSTANCE.valueOf(KEYS_COUNT + i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localSBTree.get(i));
      else
        Assert.assertEquals(localSBTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

      if (i % 2 == 0)
        Assert.assertEquals(localSBTree.get(KEYS_COUNT + i), new ORecordId((KEYS_COUNT + i) % 32000,
            OClusterPositionFactory.INSTANCE.valueOf(KEYS_COUNT + i)));
    }
  }
}
