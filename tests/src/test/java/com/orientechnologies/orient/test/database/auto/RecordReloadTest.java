package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
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

    document.setProperty("value", "value one");
    database.save(document, database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid = document.getIdentity();
    final Future<?> future =
        executor.submit(
            new Runnable() {
              @Override
              public void run() {
                ODatabaseDocument db = new ODatabaseDocumentTx(url);
                db.open("admin", "admin");

                ODocument doc = db.load(rid);
                doc.setProperty("value", "value two");
                db.save(doc);

                db.close();
              }
            });

    future.get();

    document.reload();

    Assert.assertEquals(document.getProperty("value"), "value two");
  }

  public void documentReloadLatestVersionSingleValueTwo() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final ODocument document = new ODocument();

    document.setProperty("value", "value one");
    database.save(document, database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid = document.getIdentity();
    final Future<?> future =
        executor.submit(
            new Runnable() {
              @Override
              public void run() {
                ODatabaseDocument db = new ODatabaseDocumentTx(url);
                db.open("admin", "admin");

                ODocument doc = db.load(rid);
                doc.setProperty("value", "value two");
                db.save(doc);

                db.close();
              }
            });

    future.get();

    document.reload(null, true, false);

    Assert.assertEquals(document.getProperty("value"), "value two");
  }

  public void documentReloadLatestVersionLinkedValueOne() throws Exception {
    if (!database.isRemote()) return;

    ExecutorService executor = Executors.newSingleThreadExecutor();
    final ODocument document = new ODocument();

    document.setProperty("value", "value one");

    ODocument linkedValue = new ODocument();
    linkedValue.setProperty("val", "value 1");
    database.save(linkedValue, database.getClusterNameById(database.getDefaultClusterId()));
    document.setProperty("link", linkedValue);

    database.save(document, database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid = document.getIdentity();
    final Future<?> future =
        executor.submit(
            new Runnable() {
              @Override
              public void run() {
                ODatabaseDocument db = new ODatabaseDocumentTx(url);
                db.open("admin", "admin");

                ODocument doc = db.load(rid);

                ODocument linkedValue = doc.field("link");
                linkedValue.setProperty("val", "value 2");
                db.save(linkedValue);

                db.close();
              }
            });

    future.get();

    document.reload("*:1", true);

    linkedValue = document.getProperty("link");
    Assert.assertEquals(linkedValue.getProperty("val"), "value 2");
  }

  public void documentReloadLatestVersionLinkedValueTwo() throws Exception {
    if (!database.isRemote()) return;

    ExecutorService executor = Executors.newSingleThreadExecutor();
    final ODocument document = new ODocument();

    document.setProperty("value", "value one");

    ODocument linkedValue = new ODocument();
    linkedValue.setProperty("val", "value 1");
    database.save(linkedValue, database.getClusterNameById(database.getDefaultClusterId()));
    document.setProperty("link", linkedValue);

    database.save(document, database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid = document.getIdentity();
    final Future<?> future =
        executor.submit(
            new Runnable() {
              @Override
              public void run() {
                ODatabaseDocument db = new ODatabaseDocumentTx(url);
                db.open("admin", "admin");

                ODocument doc = db.load(rid);

                ODocument linkedValue = doc.field("link");
                linkedValue.setProperty("val", "value 2");
                db.save(linkedValue);

                db.close();
              }
            });

    future.get();

    document.reload("*:1", true, false);

    linkedValue = document.getProperty("link");
    Assert.assertEquals(linkedValue.getProperty("val"), "value 1");
  }
}
