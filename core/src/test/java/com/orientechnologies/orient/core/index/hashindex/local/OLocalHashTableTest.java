package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 19.02.13
 */
@Test
public class OLocalHashTableTest {
  private static final int                   KEYS_COUNT = 500000;

  protected ODatabaseDocumentTx              databaseDocumentTx;

  protected OLocalHashTable<Integer, String> localHashTable;

  @BeforeClass
  public void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/localHashTableTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new OMurmurHash3HashFunction<Integer>();
    murmurHash3HashFunction.setValueSerializer(OIntegerSerializer.INSTANCE);

    localHashTable = new OLocalHashTable<Integer, String>(".imc", ".tsc", ".obf", ".nbh", murmurHash3HashFunction, false,
        (OAbstractPaginatedStorage) databaseDocumentTx.getStorage());

    localHashTable.create("localHashTableTest", OIntegerSerializer.INSTANCE, OBinarySerializerFactory.getInstance()
        .<String> getObjectSerializer(OType.STRING), null, true);
  }

  @AfterClass
  public void afterClass() throws Exception {
    localHashTable.clear();
    localHashTable.delete();
    databaseDocumentTx.drop();
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    localHashTable.clear();
  }

  public void testKeyPut() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localHashTable.put(i, i + "");
      Assert.assertEquals(localHashTable.get(i), i + "");
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      Assert.assertEquals(localHashTable.get(i), i + "", i + " key is absent");
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertNull(localHashTable.get(i));
    }
  }

  public void testKeyPutRandomUniform() throws IOException {
    final Set<Integer> keys = new HashSet<Integer>();
    final MersenneTwisterFast random = new MersenneTwisterFast();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      localHashTable.put(key, key + "");
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), key + "");
    }

    for (int key : keys)
      Assert.assertEquals(localHashTable.get(key), "" + key);
  }

  public void testKeyPutRandomGaussian() throws IOException {
    Set<Integer> keys = new HashSet<Integer>();
    MersenneTwisterFast random = new MersenneTwisterFast();
    keys.clear();

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

      localHashTable.put(key, key + "");
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), "" + key);
    }

    for (int key : keys)
      Assert.assertEquals(localHashTable.get(key), "" + key);
  }

  public void testKeyDeleteRandomUniform() throws IOException {
    final Set<Integer> keys = new HashSet<Integer>();
    long ms = System.currentTimeMillis();
    System.out.println("testKeyDeleteRandomUniform : " + ms);
    final MersenneTwisterFast random = new MersenneTwisterFast(ms);

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      localHashTable.put(key, key + "");
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), key + "");
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localHashTable.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }
  }

  public void testKeyDeleteRandomGaussian() throws IOException {
    HashSet<Integer> keys = new HashSet<Integer>();

    MersenneTwisterFast random = new MersenneTwisterFast();
    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

      localHashTable.put(key, key + "");
      keys.add(key);
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localHashTable.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }
  }

  public void testKeyDelete() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localHashTable.put(i, i + "");
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localHashTable.remove(i), "" + i);
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localHashTable.get(i));
      else
        Assert.assertEquals(localHashTable.get(i), i + "");
    }
  }

  public void testKeyAddDelete() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++)
      localHashTable.put(i, i + "");

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localHashTable.remove(i), i + "");

      if (i % 2 == 0)
        localHashTable.put(KEYS_COUNT + i, (KEYS_COUNT + i) + "");
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localHashTable.get(i));
      else
        Assert.assertEquals(localHashTable.get(i), i + "");

      if (i % 2 == 0)
        Assert.assertEquals(localHashTable.get(KEYS_COUNT + i), "" + (KEYS_COUNT + i));
    }
  }

  public void testKeyPutRemoveNullKey() throws IOException {
    for (int i = 0; i < 10; i++)
      localHashTable.put(i, i + "");

    localHashTable.put(null, "null");

    for (int i = 0; i < 10; i++)
      Assert.assertEquals(localHashTable.get(i), i + "");

    Assert.assertEquals(localHashTable.get(null), "null");

    for (int i = 0; i < 5; i++)
      Assert.assertEquals(localHashTable.remove(i), i + "");

    Assert.assertEquals(localHashTable.remove(null), "null");

    for (int i = 0; i < 5; i++)
      Assert.assertNull(localHashTable.remove(i));

    Assert.assertNull(localHashTable.remove(null));

    for (int i = 0; i < 5; i++)
      Assert.assertNull(localHashTable.get(i));

    Assert.assertNull(localHashTable.get(null));

    for (int i = 5; i < 10; i++)
      Assert.assertEquals(localHashTable.get(i), i + "");
  }
}
