package com.orientechnologies.orient;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.tx.OTransactionData;
import com.orientechnologies.orient.core.tx.OTransactionDataChange;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.OServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class OTransactionDataTest {

  @Test
  public void testReadWriteTransactionData() throws IOException {
    OTransactionData data = new OTransactionData(new OTransactionId(Optional.of("one"), 1, 2));
    byte[] recordData = new byte[] {1, 2, 3};
    ORecordId recordId = new ORecordId(10, 10);
    OTransactionDataChange change =
        new OTransactionDataChange(
            ORecordOperation.CREATED,
            ODocument.RECORD_TYPE,
            recordId,
            Optional.of(recordData),
            1,
            true);
    data.addChange(change);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    data.write(new DataOutputStream(outputStream));
    OTransactionData readData =
        OTransactionData.read(
            new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
    assertEquals(readData.getTransactionId(), data.getTransactionId());
    assertEquals(readData.getChanges().size(), data.getChanges().size());
    OTransactionDataChange readChange = readData.getChanges().get(0);
    assertEquals(readChange.getId(), change.getId());
    assertEquals(readChange.getType(), change.getType());
    assertEquals(readChange.getRecordType(), change.getRecordType());
    assertEquals(readChange.getVersion(), change.getVersion());
    assertArrayEquals(readChange.getRecord().get(), change.getRecord().get());
    assertEquals(readChange.isContentChanged(), change.isContentChanged());
  }

  @Test
  public void testTransactionDataChangesFromTransaction() throws IOException {
    try (final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY)) {
      try (final ODatabaseSession db =
          orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        db.createClass("test");
        db.begin();
        ODocument doc = new ODocument("test");
        db.save(doc);
        ((OTransactionInternal) db.getTransaction()).prepareSerializedOperations();
        Iterator<byte[]> res =
            ((OTransactionInternal) db.getTransaction()).getSerializedOperations();
        assertTrue(res.hasNext());
        byte[] entry = res.next();
        OTransactionDataChange readChange =
            OTransactionDataChange.deserialize(
                new DataInputStream(new ByteArrayInputStream(entry)));
        assertEquals(readChange.getId(), doc.getIdentity());
        assertEquals(readChange.getType(), ORecordOperation.CREATED);
        assertEquals(readChange.getRecordType(), ODocument.RECORD_TYPE);
        assertEquals(readChange.getVersion(), doc.getVersion());
        assertNotNull(readChange.getRecord().get());
        assertEquals(readChange.isContentChanged(), ORecordInternal.isContentChanged(doc));

        assertFalse(res.hasNext());
      }
    }
  }

  @Test
  public void testReApplyFromTransactionData()
      throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    OServer server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");

    try (OrientDB orientDB = server0.getContext()) {
      OCreateDatabaseUtil.createDatabase("test", orientDB, OCreateDatabaseUtil.TYPE_MEMORY);
      ByteArrayOutputStream backup = new ByteArrayOutputStream();
      try (final ODatabaseSession db =
          orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        db.createClass("test");
        db.begin();
        ODocument doc = new ODocument("test");
        doc.setProperty("field", "value0");
        db.save(doc);
        ODocument doc1 = new ODocument("test");
        doc1.setProperty("field", "value1");
        db.save(doc1);
        db.commit();
        ODatabaseExport export =
            new ODatabaseExport((ODatabaseDocumentInternal) db, backup, iText -> {});
        export.exportDatabase();
        export.close();
      }
      orientDB.execute(
          "create database "
              + "test1"
              + " "
              + ODatabaseType.MEMORY
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
      try (ODatabaseSession db =
          orientDB.open("test1", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        final ODatabaseImport imp =
            new ODatabaseImport(
                (ODatabaseDocumentInternal) db,
                new ByteArrayInputStream(backup.toByteArray()),
                iText -> {});
        imp.importDatabase();
        imp.close();
      }
      List<byte[]> changes = new ArrayList<>();
      try (ODatabaseSession db =
          orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        db.begin();
        ODocument doc = new ODocument("test");
        doc.setProperty("field", "value2");
        db.save(doc);
        db.command("update test set field='value3' where field='value1'").close();
        db.command("delete from test where field='value0'").close();

        ((OTransactionInternal) db.getTransaction()).prepareSerializedOperations();
        Iterator<byte[]> res =
            ((OTransactionInternal) db.getTransaction()).getSerializedOperations();
        while (res.hasNext()) {
          changes.add(res.next());
        }
        db.commit();
      }
      OTransactionId id =
          server0.getDistributedManager().getMessageService().getDatabase("test1").nextId().get();
      server0.getDistributedManager().getMessageService().getDatabase("test1").rollback(id);
      OTransactionData data = new OTransactionData(id);
      for (byte[] change : changes) {
        data.addRecord(change);
      }
      try (final ODatabaseSession db =
          orientDB.open("test1", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        ((ODatabaseDocumentInternal) db).syncCommit(data);

        assertEquals(2, db.countClass("test"));
        try (OResultSet r = db.query("select count(*) as count from test where field='value3'")) {
          assertEquals((Long) 1L, r.next().getProperty("count"));
        }
        try (OResultSet r = db.query("select count(*) as count from test where field='value2'")) {
          assertEquals((Long) 1L, r.next().getProperty("count"));
        }
        try (OResultSet r = db.query("select count(*) as count from test where field='value0'")) {
          assertEquals((Long) 0L, r.next().getProperty("count"));
        }
      }
    }
    server0.shutdown();
  }
}
