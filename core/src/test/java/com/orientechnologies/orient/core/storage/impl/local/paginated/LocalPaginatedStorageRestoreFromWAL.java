package com.orientechnologies.orient.core.storage.impl.local.paginated;

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

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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

/**
 * @author Andrey Lomakin
 * @since 29.05.13
 */
@Test
public class LocalPaginatedStorageRestoreFromWAL {
  private ODatabaseDocumentTx testDocumentTx;
  private ODatabaseDocumentTx baseDocumentTx;
  private File                buildDir;

  private ExecutorService     executorService = Executors.newCachedThreadPool();

  @BeforeClass
  public void beforeClass() {
    OGlobalConfiguration.MVRBTREE_RID_BINARY_THRESHOLD.setValue(-1);
    OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.setValue("nothing");

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageRestoreFromWAL";

    buildDir = new File(buildDirectory);
    if (buildDir.exists())
      buildDir.delete();

    buildDir.mkdir();
  }

  @AfterClass
  public void afterClass() {
    buildDir.delete();
  }

  @BeforeMethod
  public void beforeMethod() {
    baseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageRestoreFromWAL");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    createSchema(baseDocumentTx);
  }

  @AfterMethod
  public void afterMethod() {
    testDocumentTx.open("admin", "admin");
    testDocumentTx.drop();

    baseDocumentTx.open("admin", "admin");
    baseDocumentTx.drop();
  }

  public void testSimpleRestore() throws Exception {
    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    baseDocumentTx.declareIntent(new OIntentMassiveInsert());

    for (int i = 0; i < 5; i++)
      futures.add(executorService.submit(new DataPropagationTask()));

    for (Future<Void> future : futures)
      future.get();

    Thread.sleep(1500);
    copyDataFromTestWithoutClose();
    OStorage baseStorage = baseDocumentTx.getStorage();
    baseDocumentTx.close();
    baseStorage.close();

    testDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageRestoreFromWAL");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    ODatabaseCompare databaseCompare = new ODatabaseCompare(testDocumentTx.getURL(), baseDocumentTx.getURL(), "admin", "admin",
        new OCommandOutputListener() {
          @Override
          public void onMessage(String text) {
            System.out.println(text);
          }
        });

    Assert.assertTrue(databaseCompare.compare());
  }

  private void copyDataFromTestWithoutClose() throws Exception {
    final String testStoragePath = baseDocumentTx.getURL().substring("plocal:".length());
    final String copyTo = buildDir.getAbsolutePath() + File.separator + "testLocalPaginatedStorageRestoreFromWAL";

    final File testStorageDir = new File(testStoragePath);
    final File copyToDir = new File(copyTo);

    Assert.assertTrue(!copyToDir.exists());
    Assert.assertTrue(copyToDir.mkdir());

    File[] storageFiles = testStorageDir.listFiles();
    Assert.assertNotNull(storageFiles);

    for (File storageFile : storageFiles) {
      String copyToPath;
      if (storageFile.getAbsolutePath().endsWith("baseLocalPaginatedStorageRestoreFromWAL.wmr"))
        copyToPath = copyToDir.getAbsolutePath() + File.separator + "testLocalPaginatedStorageRestoreFromWAL.wmr";
      else if (storageFile.getAbsolutePath().endsWith("baseLocalPaginatedStorageRestoreFromWAL.0.wal"))
        copyToPath = copyToDir.getAbsolutePath() + File.separator + "testLocalPaginatedStorageRestoreFromWAL.0.wal";
      else
        copyToPath = copyToDir.getAbsolutePath() + File.separator + storageFile.getName();

      copyFile(storageFile.getAbsolutePath(), copyToPath);
    }
  }

  private void createSchema(ODatabaseDocumentTx databaseDocumentTx) {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocumentTx);

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass testOneClass = schema.createClass("TestOne");

    testOneClass.createProperty("intProp", OType.INTEGER);
    testOneClass.createProperty("stringProp", OType.STRING);
    testOneClass.createProperty("stringSet", OType.EMBEDDEDSET, OType.STRING);
    testOneClass.createProperty("linkMap", OType.LINKMAP, OType.STRING);

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
}
