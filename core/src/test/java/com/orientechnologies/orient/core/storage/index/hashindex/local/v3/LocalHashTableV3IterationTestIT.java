package com.orientechnologies.orient.core.storage.index.hashindex.local.v3;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashTable;
import org.junit.*;

import java.util.*;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 13.03.13
 */
public class LocalHashTableV3IterationTestIT {
  private static final int KEYS_COUNT = 500000;

  private ODatabaseDocumentTx databaseDocumentTx;

  private OLocalHashTableV3<Integer, String> localHashTable;

  @Before
  public void beforeClass() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/localHashTableV3IterationTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    OHashFunction<Integer> hashFunction = value -> Long.MAX_VALUE / 2 + value;

    localHashTable = new OLocalHashTableV3<>("localHashTableIterationTest", ".imc", ".tsc", ".obf", ".nbh",
        (OAbstractPaginatedStorage) databaseDocumentTx.getStorage());

    localHashTable
        .create(OIntegerSerializer.INSTANCE, OBinarySerializerFactory.getInstance().<String>getObjectSerializer(OType.STRING), null,
            null, hashFunction, true);
  }

  @After
  public void afterClass() throws Exception {
    localHashTable.clear();
    localHashTable.delete();
    databaseDocumentTx.drop();
  }

  @After
  public void afterMethod() throws Exception {
    localHashTable.clear();
  }

  @Test
  public void testNextHaveRightOrder() throws Exception {
    SortedSet<Integer> keys = new TreeSet<Integer>();
    keys.clear();
    final Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      if (localHashTable.get(key) == null) {
        localHashTable.put(key, key + "");
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), "" + key);
      }
    }

    OHashTable.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(Integer.MIN_VALUE);
    int curPos = 0;
    for (int key : keys) {
      int sKey = entries[curPos].key;

      Assert.assertEquals(key, sKey);
      curPos++;
      if (curPos >= entries.length) {
        entries = localHashTable.higherEntries(entries[entries.length - 1].key);
        curPos = 0;
      }
    }
  }

  public void testNextSkipsRecordValid() throws Exception {
    List<Integer> keys = new ArrayList<Integer>();
    keys.clear();

    final Random random = new Random();
    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      if (localHashTable.get(key) == null) {
        localHashTable.put(key, key + "");
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), "" + key);
      }
    }

    Collections.sort(keys);

    OHashTable.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(keys.get(10));
    int curPos = 0;
    for (int key : keys) {
      if (key < keys.get(10)) {
        continue;
      }
      int sKey = entries[curPos].key;
      Assert.assertEquals(key, sKey);

      curPos++;
      if (curPos >= entries.length) {
        entries = localHashTable.higherEntries(entries[entries.length - 1].key);
        curPos = 0;
      }
    }
  }

  @Test
  @Ignore
  public void testNextHaveRightOrderUsingNextMethod() throws Exception {
    List<Integer> keys = new ArrayList<Integer>();
    keys.clear();
    Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      if (localHashTable.get(key) == null) {
        localHashTable.put(key, key + "");
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }

    Collections.sort(keys);

    for (int key : keys) {
      OHashTable.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(key);
      Assert.assertTrue(key == entries[0].key);
    }

    for (int j = 0, keysSize = keys.size() - 1; j < keysSize; j++) {
      int key = keys.get(j);
      int sKey = localHashTable.higherEntries(key)[0].key;
      Assert.assertTrue(sKey == keys.get(j + 1));
    }
  }

}
