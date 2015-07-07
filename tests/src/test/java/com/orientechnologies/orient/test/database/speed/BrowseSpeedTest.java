package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.TimerTask;

/**
 * Test the speed on browsing records at storage level. Run this with the following syntax:
 * 
 * <pre>
 * BrowseSpeedTest <directory where the database is stored> <class to use for browsing>
 * </pre>
 * 
 * @author Luca Garulli
 * @since 9/17/14
 */
@Test
public class BrowseSpeedTest {
  private static String className;
  private static String url;

  public static void main(String[] args) throws IOException {
    if (args.length < 2)
      throw new IllegalArgumentException("Syntax error: ");

    url = "plocal:" + args[0];
    className = args[1];

    new BrowseSpeedTest().testIterationSpeed();
  }

  public void testIterationSpeed() throws IOException {
    Orient.instance().scheduleTask(new TimerTask() {
      @Override
      public void run() {
        final OAbstractPaginatedStorage stg = (OAbstractPaginatedStorage) Orient.instance().getStorages().iterator().next();
        System.out.println("DiskCache used: " + stg.getReadCache().getUsedMemory());
      }
    }, 1000, 1000);

    browseStorageClusters();
    System.out.println("2nd lap...");
    browseStorageClusters();
    System.out.println("3rd lap...");
    browseStorageClusters();
    System.out.println("4th lap...");
    browseStorageClusters();

    // browseClusters();
    // browseClusters();
    // loadAllRecordsOneByOne();
    // loadAllRecordsOneByOne();
  }

  protected void browseStorageClusters() throws IOException {
    ODatabaseDocumentTx db = openDatabase();
    final long total = db.countClass(className);

    final OClass cls = db.getMetadata().getSchema().getClass(className);
    final int[] clIds = cls.getPolymorphicClusterIds();

    long start = System.currentTimeMillis();

    int loaded = 0;

    for (int clId : clIds) {
      OCluster cluster = db.getStorage().getClusterById(clId);
      final long clusterRecords = cluster.getEntries();
      for (long rid = 0; rid < clusterRecords; ++rid) {
        final ORawBuffer buffer = cluster.readRecord(rid);
        loaded++;
      }
    }

    long end = System.currentTimeMillis();
    System.out.println("Browse clusters " + total + " and loaded " + loaded + " took " + (end - start));

    db.close();
  }

  protected void browseClusters() {
    ODatabaseDocumentTx db = openDatabase();
    final long total = db.countClass(className);

    ORecordIteratorClass iterator = new ORecordIteratorClass(db, db, className, true);

    long start = System.currentTimeMillis();

    int loaded = 0;

    ORecord rec;
    while (iterator.hasNext()) {
      rec = iterator.next();
      if (rec != null)
        loaded++;
    }

    long end = System.currentTimeMillis();
    System.out.println("Iterator " + total + " and loaded " + loaded + " took " + (end - start));

    db.close();
  }

  protected void loadAllRecordsOneByOne() {
    ODatabaseDocumentTx db = openDatabase();
    final long total = db.countClass(className);

    long start = System.currentTimeMillis();

    final int clusterId = db.getClusterIdByName(className);

    int loaded = 0;
    for (int i = 0; i < total; ++i) {
      if (db.load(new ORecordId(clusterId, i)) != null)
        loaded++;
    }

    long end = System.currentTimeMillis();
    System.out.println("Direct loading " + total + " and loaded " + loaded + " took " + (end - start));

    db.close();
  }

  protected ODatabaseDocumentTx openDatabase() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open("admin", "admin");
    return db;
  }
}
