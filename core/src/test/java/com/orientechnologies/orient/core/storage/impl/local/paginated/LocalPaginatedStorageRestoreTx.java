package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 14.06.13
 */
public class LocalPaginatedStorageRestoreTx {
  private ODatabaseDocumentTx testDocumentTx;
  private ODatabaseDocumentTx baseDocumentTx;
  private File buildDir;

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

  @Before
  public void beforeClass() {
    OGlobalConfiguration.FILE_LOCK.setValue(false);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageRestoreFromTx";

    buildDir = new File(buildDirectory);
    if (buildDir.exists()) buildDir.delete();

    buildDir.mkdir();

    baseDocumentTx =
        new ODatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageRestoreFromTx");
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

    buildDir.delete();
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
    OStorage storage = baseDocumentTx.getStorage();
    baseDocumentTx.close();
    storage.close();

    testDocumentTx =
        new ODatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageRestoreFromTx");
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
        buildDir.getAbsolutePath() + File.separator + "testLocalPaginatedStorageRestoreFromTx";

    final File testStorageDir = new File(testStoragePath);
    final File copyToDir = new File(copyTo);

    Assert.assertTrue(!copyToDir.exists());
    Assert.assertTrue(copyToDir.mkdir());

    File[] storageFiles = testStorageDir.listFiles();
    Assert.assertNotNull(storageFiles);

    for (File storageFile : storageFiles) {
      String copyToPath;
      if (storageFile.getAbsolutePath().endsWith("baseLocalPaginatedStorageRestoreFromTx.wmr"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.wmr";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.0.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.0.wal";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.1.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.1.wal";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.2.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.2.wal";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.3.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.3.wal";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.4.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.4.wal";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.5.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.5.wal";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.6.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.6.wal";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.7.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.7.wal";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.8.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.8.wal";
      else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.9.wal"))
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.9.wal";
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
      int rollbacksCount = 0;
      try {
        List<ORID> secondDocs = new ArrayList<ORID>();
        List<ORID> firstDocs = new ArrayList<ORID>();

        OClass classOne = db.getMetadata().getSchema().getClass("TestOne");
        OClass classTwo = db.getMetadata().getSchema().getClass("TestTwo");

        for (int i = 0; i < 20000; i++) {
          try {
            db.begin(OTransaction.TXTYPE.OPTIMISTIC);

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

            ODocument docTwo = null;

            if (random.nextBoolean()) {
              docTwo = new ODocument(classTwo);

              List<String> stringList = new ArrayList<String>();

              for (int n = 0; n < 5; n++) {
                stringList.add("strnd" + random.nextInt());
              }

              docTwo.field("stringList", stringList);
              docTwo.save();
            }

            if (!secondDocs.isEmpty()) {
              int startIndex = random.nextInt(secondDocs.size());
              int endIndex = random.nextInt(secondDocs.size() - startIndex) + startIndex;

              Map<String, ORID> linkMap = new HashMap<String, ORID>();

              for (int n = startIndex; n < endIndex; n++) {
                ORID docTwoRid = secondDocs.get(n);
                linkMap.put(docTwoRid.toString(), docTwoRid);
              }

              docOne.field("linkMap", linkMap);
              docOne.save();
            }

            int deleteIndex = -1;
            if (!firstDocs.isEmpty()) {
              boolean deleteDoc = random.nextDouble() <= 0.2;

              if (deleteDoc) {
                deleteIndex = random.nextInt(firstDocs.size());
                if (deleteIndex >= 0) {
                  ORID rid = firstDocs.get(deleteIndex);
                  db.delete(rid);
                }
              }
            }

            if (!secondDocs.isEmpty() && (random.nextDouble() <= 0.2)) {
              ODocument conflictDocTwo = new ODocument();
              ORecordInternal.setIdentity(conflictDocTwo, new ORecordId(secondDocs.get(0)));
              conflictDocTwo.setDirty();
              conflictDocTwo.save();
            }

            db.commit();

            if (deleteIndex >= 0) firstDocs.remove(deleteIndex);

            firstDocs.add(docOne.getIdentity());
            if (docTwo != null) secondDocs.add(docTwo.getIdentity());

          } catch (Exception e) {
            db.rollback();
            rollbacksCount++;
          }
        }
      } finally {
        db.close();
      }

      System.out.println("Rollbacks count " + rollbacksCount);
      return null;
    }
  }
}
