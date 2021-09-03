package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class RecordReloadTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public RecordReloadTest(@Optional String url) {
    super(url);
  }

  public void documentReloadLatestVersionSingleValueOne() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final ODocument document = new ODocument();

    document.field("value", "value one");
    document.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid = document.getIdentity();
    final Future<?> future =
        executor.submit(
            new Runnable() {
              @Override
              public void run() {
                ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
                db.open("admin", "admin");

                ODocument doc = db.load(rid);
                doc.field("value", "value two");
                doc.save();

                db.close();
              }
            });

    future.get();

    document.reload();

    Assert.assertEquals(document.field("value"), "value two");
  }

  public void documentReloadLatestVersionSingleValueTwo() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final ODocument document = new ODocument();

    document.field("value", "value one");
    document.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid = document.getIdentity();
    final Future<?> future =
        executor.submit(
            new Runnable() {
              @Override
              public void run() {
                ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
                db.open("admin", "admin");

                ODocument doc = db.load(rid);
                doc.field("value", "value two");
                doc.save();

                db.close();
              }
            });

    future.get();

    document.reload(null, true, false);

    Assert.assertEquals(document.field("value"), "value two");
  }

  public void documentReloadLatestVersionLinkedValueOne() throws Exception {
    if (!database.isRemote()) return;

    ExecutorService executor = Executors.newSingleThreadExecutor();
    final ODocument document = new ODocument();

    document.field("value", "value one");

    ODocument linkedValue = new ODocument();
    linkedValue.field("val", "value 1");
    linkedValue.save(database.getClusterNameById(database.getDefaultClusterId()));
    document.field("link", linkedValue);

    document.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid = document.getIdentity();
    final Future<?> future =
        executor.submit(
            new Runnable() {
              @Override
              public void run() {
                ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
                db.open("admin", "admin");

                ODocument doc = db.load(rid);

                ODocument linkedValue = doc.field("link");
                linkedValue.field("val", "value 2");
                linkedValue.save();

                db.close();
              }
            });

    future.get();

    document.reload("*:1", true);

    linkedValue = document.field("link");
    Assert.assertEquals(linkedValue.field("val"), "value 2");
  }

  public void documentReloadLatestVersionLinkedValueTwo() throws Exception {
    if (!database.isRemote()) return;

    ExecutorService executor = Executors.newSingleThreadExecutor();
    final ODocument document = new ODocument();

    document.field("value", "value one");

    ODocument linkedValue = new ODocument();
    linkedValue.field("val", "value 1");
    linkedValue.save(database.getClusterNameById(database.getDefaultClusterId()));
    document.field("link", linkedValue);

    document.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid = document.getIdentity();
    final Future<?> future =
        executor.submit(
            new Runnable() {
              @Override
              public void run() {
                ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
                db.open("admin", "admin");

                ODocument doc = db.load(rid);

                ODocument linkedValue = doc.field("link");
                linkedValue.field("val", "value 2");
                linkedValue.save();

                db.close();
              }
            });

    future.get();

    document.reload("*:1", true, false);

    linkedValue = document.field("link");
    Assert.assertEquals(linkedValue.field("val"), "value 1");
  }
}
