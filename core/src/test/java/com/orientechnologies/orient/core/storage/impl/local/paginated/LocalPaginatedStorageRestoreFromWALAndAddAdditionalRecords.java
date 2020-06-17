package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 18.06.13
 */
public class LocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords {
  private static File buildDir;
  private ODatabaseDocumentTx testDocumentTx;
  private ODatabaseDocumentTx baseDocumentTx;
  private ExecutorService executorService = Executors.newCachedThreadPool();

  @BeforeClass
  public static void beforeClass() {
    OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.setValue("nothing");
    OGlobalConfiguration.FILE_LOCK.setValue(false);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageRestoreFromWALAndAddAdditionalRecords";

    buildDir = new File(buildDirectory);
    if (buildDir.exists()) buildDir.delete();

    buildDir.mkdir();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    //    Files.delete(buildDir.toPath());
    OFileUtils.deleteRecursively(buildDir);
  }

  @Before
  public void beforeMethod() throws IOException {
    OFileUtils.deleteRecursively(buildDir);

    baseDocumentTx =
        new ODatabaseDocumentTx(
            "plocal:"
                + buildDir.getAbsolutePath()
                + "/baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    createSchema(baseDocumentTx);
  }

  @After
  public void afterMethod() {
    testDocumentTx.open("admin", "admin");
    testDocumentTx.drop();

    baseDocumentTx.open("admin", "admin");
    baseDocumentTx.drop();
  }

  @Test
  @Ignore
  public void testRestoreAndAddNewItems() throws Exception {
    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    baseDocumentTx.declareIntent(new OIntentMassiveInsert());

    Random random = new Random();

    long[] seeds = new long[5];
    for (int i = 0; i < 5; i++) {
      seeds[i] = random.nextLong();
      System.out.println("Seed [" + i + "] = " + seeds[i]);
    }

    for (long seed : seeds) futures.add(executorService.submit(new DataPropagationTask(seed)));

    for (Future<Void> future : futures) future.get();

    futures.clear();

    Thread.sleep(1500);
    copyDataFromTestWithoutClose();
    OStorage storage = baseDocumentTx.getStorage();
    baseDocumentTx.close();
    storage.close();

    testDocumentTx =
        new ODatabaseDocumentTx(
            "plocal:"
                + buildDir.getAbsolutePath()
                + "/testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    long dataAddSeed = random.nextLong();
    System.out.println("Data add seed = " + dataAddSeed);
    for (int i = 0; i < 1; i++)
      futures.add(executorService.submit(new DataPropagationTask(dataAddSeed)));

    for (Future<Void> future : futures) future.get();

    ODatabaseCompare databaseCompare =
        new ODatabaseCompare(
            testDocumentTx.getURL(),
            baseDocumentTx.getURL(),
            "admin",
            "admin",
            new OCommandOutputListener() {
              @Override
              public void onMessage(String text) {
                System.out.println(text);
              }
            });
    databaseCompare.setCompareIndexMetadata(true);

    Assert.assertTrue(databaseCompare.compare());
  }

  private void copyDataFromTestWithoutClose() throws Exception {
    final Path testStoragePath = Paths.get(baseDocumentTx.getURL().substring("plocal:".length()));
    Path buildPath = Paths.get(buildDir.toURI());

    final Path copyTo =
        buildPath.resolve("testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords");

    Files.copy(testStoragePath, copyTo);

    Files.walkFileTree(
        testStoragePath,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Path fileToCopy = copyTo.resolve(testStoragePath.relativize(file));
            if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.wmr"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.wmr");
            else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.0.wal"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.0.wal");
            else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.1.wal"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.1.wal");
            else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.2.wal"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.2.wal");
            else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.3.wal"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.3.wal");
            else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.4.wal"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.4.wal");
            else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.5.wal"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.5.wal");
            else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.6.wal"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.6.wal");
            else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.7.wal"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.7.wal");
            else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.8.wal"))
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.8.wal");

            if (fileToCopy.endsWith("dirty.fl")) return FileVisitResult.CONTINUE;

            Files.copy(file, fileToCopy);

            return FileVisitResult.CONTINUE;
          }
        });
  }

  private void createSchema(ODatabaseDocumentTx databaseDocumentTx) {
    ODatabaseRecordThreadLocal.instance().set(databaseDocumentTx);

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass testOneClass = schema.createClass("TestOne");

    testOneClass.createProperty("intProp", OType.INTEGER);
    testOneClass.createProperty("stringProp", OType.STRING);
    testOneClass.createProperty("stringSet", OType.EMBEDDEDSET, OType.STRING);
    testOneClass.createProperty("linkMap", OType.LINKMAP);

    OClass testTwoClass = schema.createClass("TestTwo");

    testTwoClass.createProperty("stringList", OType.EMBEDDEDLIST, OType.STRING);
  }

  public class DataPropagationTask implements Callable<Void> {
    private ODatabaseDocumentTx baseDB;
    private ODatabaseDocumentTx testDB;
    private long seed;

    public DataPropagationTask(long seed) {
      this.seed = seed;

      baseDB = new ODatabaseDocumentTx(baseDocumentTx.getURL());
      baseDB.open("admin", "admin");

      if (testDocumentTx != null) {
        testDB = new ODatabaseDocumentTx(testDocumentTx.getURL());
        testDB.open("admin", "admin");
      }
    }

    @Override
    public Void call() throws Exception {

      Random random = new Random(seed);

      ODatabaseRecordThreadLocal.instance().set(baseDB);

      try {
        List<ORID> testTwoList = new ArrayList<ORID>();
        List<ORID> firstDocs = new ArrayList<ORID>();

        OClass classOne = baseDB.getMetadata().getSchema().getClass("TestOne");
        OClass classTwo = baseDB.getMetadata().getSchema().getClass("TestTwo");

        for (int i = 0; i < 10000; i++) {
          ODocument docOne = new ODocument(classOne);
          docOne.field("intProp", random.nextInt());

          byte[] stringData = new byte[256];
          random.nextBytes(stringData);
          String stringProp = new String(stringData);

          docOne.field("stringProp", stringProp);

          Set<String> stringSet = new HashSet<String>();
          for (int n = 0; n < 5; n++) {
            stringSet.add("str" + random.nextInt());
          }
          docOne.field("stringSet", stringSet);

          saveDoc(docOne);

          firstDocs.add(docOne.getIdentity());

          if (random.nextBoolean()) {
            ODocument docTwo = new ODocument(classTwo);

            List<String> stringList = new ArrayList<String>();

            for (int n = 0; n < 5; n++) {
              stringList.add("strnd" + random.nextInt());
            }

            docTwo.field("stringList", stringList);
            saveDoc(docTwo);
            testTwoList.add(docTwo.getIdentity());
          }

          if (!testTwoList.isEmpty()) {
            int startIndex = random.nextInt(testTwoList.size());
            int endIndex = random.nextInt(testTwoList.size() - startIndex) + startIndex;

            Map<String, ORID> linkMap = new HashMap<String, ORID>();

            for (int n = startIndex; n < endIndex; n++) {
              ORID docTwoRid = testTwoList.get(n);
              linkMap.put(docTwoRid.toString(), docTwoRid);
            }

            docOne.field("linkMap", linkMap);

            saveDoc(docOne);
          }

          boolean deleteDoc = random.nextDouble() <= 0.2;
          if (deleteDoc) {
            ORID rid = firstDocs.remove(random.nextInt(firstDocs.size()));

            deleteDoc(rid);
          }
        }
      } finally {
        baseDB.close();
        if (testDB != null) testDB.close();
      }

      return null;
    }

    private void saveDoc(ODocument document) {
      ODatabaseRecordThreadLocal.instance().set(baseDB);

      ODocument testDoc = new ODocument();
      document.copyTo(testDoc);
      document.save();

      if (testDB != null) {
        ODatabaseRecordThreadLocal.instance().set(testDB);
        testDoc.save();

        Assert.assertEquals(testDoc.getIdentity(), document.getIdentity());

        ODatabaseRecordThreadLocal.instance().set(baseDB);
      }
    }

    private void deleteDoc(ORID rid) {
      baseDB.delete(rid);

      if (testDB != null) {
        ODatabaseRecordThreadLocal.instance().set(testDB);
        Assert.assertNotNull(testDB.load(rid));
        testDB.delete(rid);
        Assert.assertNull(testDB.load(rid));
        ODatabaseRecordThreadLocal.instance().set(baseDB);
      }
    }
  }
}
