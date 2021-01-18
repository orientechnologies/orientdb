package com.orientechnologies.orient.core.db.conflict;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class OMultithreadConflictManagementTest {

  @Test
  public void testAutomergeConflictStrategyThreaded() {
    final ODatabaseDocument db =
        new ODatabaseDocumentTx(
            "memory:" + OMultithreadConflictManagementTest.class.getSimpleName());
    db.create();
    db.setConflictStrategy("automerge");
    try {
      db.begin();
      ORidBag bag = new ORidBag();
      bag.add(new ORecordId(30, 20));
      ODocument doc = new ODocument();
      doc.field("bag", bag);
      doc = db.save(doc, db.getClusterNameById(db.getDefaultClusterId()));
      db.commit();
      final ORID id;
      id = doc.getIdentity();
      ExecutorService service = Executors.newFixedThreadPool(2);
      final AtomicInteger interger = new AtomicInteger(0);
      Runnable runnable =
          new Runnable() {

            @Override
            public void run() {
              ODatabaseDocument db =
                  new ODatabaseDocumentTx(
                      "memory:" + OMultithreadConflictManagementTest.class.getSimpleName());
              db.setConflictStrategy("automerge");
              db.open("admin", "admin");
              db.begin();
              ODocument doc = db.load(id);
              ORidBag bag1 = ((ORidBag) doc.field("bag"));
              ORecordId newId = new ORecordId(30, 30 + interger.incrementAndGet());
              bag1.add(newId);
              db.save(doc);
              db.commit();
              db.close();
              ORidBag bag = doc.field("bag");
              bag.setAutoConvertToRecord(false);
            }
          };
      service.execute(runnable);
      service.execute(runnable);

      service.shutdown();
      try {
        service.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
      }
      db.getLocalCache().clear();
      doc = db.load(id);

      ORidBag bag2 = doc.field("bag");
      bag2.setAutoConvertToRecord(false);
      List<ORecordId> ids =
          new ArrayList<ORecordId>(
              Arrays.asList(new ORecordId(30, 20), new ORecordId(30, 31), new ORecordId(30, 32)));
      for (OIdentifiable ide : bag2) {
        assertTrue(ids.remove(ide));
      }

      assertTrue(ids.isEmpty());

    } finally {
      db.drop();
    }
  }
}
