package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by lomak_000 on 03.07.2015.
 */
public class BrowseSpeedTest {
  private static final String PATH  = "plocal:F:\\pokec";
  private static final String CLASS = "Profile";

  @Test
  public void testIterationSpeed() throws Exception {
    int counter = 0;

    while (true) {
      browseStorageClusters();
      counter++;
      if (counter % 10 == 0) {
        System.out.println("Start sleep for 5 sec");
        Thread.sleep(5000);
        System.out.println("Stop sleep");
      }
    }
  }

  protected void browseStorageClusters() throws IOException {
    ODatabaseDocumentTx db = openDatabase();
    final long total = db.countClass(CLASS);

    final OClass cls = db.getMetadata().getSchema().getClass(CLASS);
    final int[] clIds = cls.getPolymorphicClusterIds();

    long start = System.currentTimeMillis();

    int loaded = 0;

    ORecord rec;
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
    final long total = db.countClass(CLASS);

    ORecordIteratorClass iterator = new ORecordIteratorClass(db, db, CLASS, true);

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
    final long total = db.countClass(CLASS);

    long start = System.currentTimeMillis();

    final int clusterId = db.getClusterIdByName(CLASS);

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
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(PATH);
    db.open("admin", "admin");
    return db;
  }
}
