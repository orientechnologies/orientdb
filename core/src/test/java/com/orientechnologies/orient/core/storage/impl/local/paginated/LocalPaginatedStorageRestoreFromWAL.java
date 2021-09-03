package com.orientechnologies.orient.core.storage.impl.local.paginated;

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
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
 * @since 29.05.13
 */
public class LocalPaginatedStorageRestoreFromWAL {
  private static File buildDir;
  private ODatabaseDocumentTx testDocumentTx;
  private ODatabaseDocumentTx baseDocumentTx;
  private ExecutorService executorService = Executors.newCachedThreadPool();

  private static void copyFile(String from, String to) throws IOException {
    final File fromFile = new File(from);
    FileInputStream fromInputStream = new FileInputStream(fromFile);
    BufferedInputStream fromBufferedStream = new BufferedInputStream(fromInputStream);

    FileOutputStream toOutputStream = new FileOutputStream(to);
    byte[] data = new byte[1024];
    int bytesRead = fromBufferedStream.read(data);
    while (bytesRead > 0) {
      toOutputStream.write(data, 0, bytesRead);
      bytesRead = fromBufferedStream.read(data);
    }

    fromBufferedStream.close();
    toOutputStream.close();
  }

  @BeforeClass
  public static void beforeClass() {
    OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.setValue("nothing");
    OGlobalConfiguration.FILE_LOCK.setValue(false);
    OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(100000000);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageRestoreFromWAL";

    buildDir = new File(buildDirectory);
    if (buildDir.exists()) buildDir.delete();

    buildDir.mkdir();
  }

  @AfterClass
  public static void afterClass() {
    buildDir.delete();
  }

  @Before
  public void beforeMethod() {
    baseDocumentTx =
        new ODatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageRestoreFromWAL");
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
  public void testSimpleRestore() throws Exception {
    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    baseDocumentTx.declareIntent(new OIntentMassiveInsert());

    for (int i = 0; i < 8; i++) futures.add(executorService.submit(new DataPropagationTask()));

    for (Future<Void> future : futures) future.get();

    Thread.sleep(1500);
    copyDataFromTestWithoutClose();
    OStorage baseStorage = baseDocumentTx.getStorage();
    baseDocumentTx.close();
    baseStorage.close();

    testDocumentTx =
        new ODatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageRestoreFromWAL");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

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
    final String testStoragePath = baseDocumentTx.getURL().substring("plocal:".length());
    final String copyTo =
        buildDir.getAbsolutePath() + File.separator + "testLocalPaginatedStorageRestoreFromWAL";

    final File testStorageDir = new File(testStoragePath);
    final File copyToDir = new File(copyTo);

    Assert.assertTrue(!copyToDir.exists());
    Assert.assertTrue(copyToDir.mkdir());

    File[] storageFiles = testStorageDir.listFiles();
    Assert.assertNotNull(storageFiles);

    for (File storageFile : storageFiles) {
      String copyToPath;
      if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.wmr"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.wmr";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.0.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.0.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.1.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.1.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.2.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.2.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.3.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.3.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.4.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.4.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.5.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.5.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.6.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.6.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.7.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.7.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.8.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.8.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.9.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.9.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.10.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.10.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.11.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.11.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.12.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.12.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.13.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.13.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.14.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.14.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.15.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.15.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.16.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.16.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.17.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.17.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.18.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.18.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.19.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.19.wal";
      else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.20.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.20.wal";
      else copyToPath = copyToDir.getAbsolutePath() + File.separator + storageFile.getName();

      if (storageFile.getName().equals("dirty.fl")) continue;

      copyFile(storageFile.getAbsolutePath(), copyToPath);
    }
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
    @Override
    public Void call() throws Exception {

      Random random = new Random();

      final ODatabaseDocumentTx db = new ODatabaseDocumentTx(baseDocumentTx.getURL());
      db.open("admin", "admin");
      try {
        List<ORID> testTwoList = new ArrayList<ORID>();
        List<ORID> firstDocs = new ArrayList<ORID>();

        OClass classOne = db.getMetadata().getSchema().getClass("TestOne");
        OClass classTwo = db.getMetadata().getSchema().getClass("TestTwo");

        for (int i = 0; i < 5000; i++) {
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

          docOne.save();

          firstDocs.add(docOne.getIdentity());

          if (random.nextBoolean()) {
            ODocument docTwo = new ODocument(classTwo);

            List<String> stringList = new ArrayList<String>();

            for (int n = 0; n < 5; n++) {
              stringList.add("strnd" + random.nextInt());
            }

            docTwo.field("stringList", stringList);
            docTwo.save();

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
            docOne.save();
          }

          boolean deleteDoc = random.nextDouble() <= 0.2;
          if (deleteDoc) {
            ORID rid = firstDocs.remove(random.nextInt(firstDocs.size()));
            db.delete(rid);
          }
        }
      } finally {
        db.close();
      }

      return null;
    }
  }
}
