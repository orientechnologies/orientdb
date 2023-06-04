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
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import org.junit.*;

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
    final int keysCount = 40_000;

    String[] lastKey = new String[1];
    for (int i = 0; i < keysCount; i++) {
      final int iteration = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final String key = Integer.toString(iteration);
            if (iteration == 28017) {
              Assert.assertEquals(
                  267 + " key is absent",
                  new ORecordId(267 % 32000, 267),
                  binaryBTree.get(stringToLexicalBytes(Integer.toString(267))));
            }
            binaryBTree.put(
                atomicOperation,
                stringToLexicalBytes(key),
                new ORecordId(iteration % 32000, iteration));

            if (iteration == 28017) {
              Assert.assertEquals(
                      267 + " key is absent",
                      new ORecordId(267 % 32000, 267),
                      binaryBTree.get(stringToLexicalBytes(Integer.toString(267))));
            }

            if ((iteration + 1) % 100_000 == 0) {
              System.out.printf("%d items loaded out of %d%n", iteration + 1, keysCount);
            }

            if (lastKey[0] == null) {
              lastKey[0] = key;
            } else if (key.compareTo(lastKey[0]) > 0) {
              lastKey[0] = key;
            }
          });

      //      Assert.assertArrayEquals(stringToLexicalBytes("0"), binaryBTree.firstKey());
      //      Assert.assertArrayEquals(stringToLexicalBytes(lastKey[0]), binaryBTree.lastKey());
    }

    System.out.println(
        "First key len "
            + binaryBTree.firstKey().length
            + " vs "
            + stringToLexicalBytes("0").length);
    System.out.println(
        "Last key len "
            + binaryBTree.lastKey().length
            + " vs "
            + stringToLexicalBytes(lastKey[0]).length);

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


  private static byte[] stringToLexicalBytes(final String value) {
    return keyNormalizers.normalize(new OCompositeKey(value), types);
  }

  enum Operation {
    INSERT,
    DELETE
  }
}
