package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

import java.io.*;
import java.util.Iterator;
import java.util.Optional;

import static org.junit.Assert.*;

public class OTransactionDataTest {

  @Test
  public void testReadWriteTransactionData() throws IOException {

    OTransactionData data = new OTransactionData(new OTransactionId(Optional.of("one"), 1, 2));
    byte[] recordData = new byte[] { 1, 2, 3 };
    ORecordId recordId = new ORecordId(10, 10);
    OTransactionDataChange change = new OTransactionDataChange(ORecordOperation.CREATED, ODocument.RECORD_TYPE, recordId,
        Optional.of(recordData), 1, true);
    data.addChange(change);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    data.write(new DataOutputStream(outputStream));
    OTransactionData readData = OTransactionData.read(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
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

    try (OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDB.create("test", ODatabaseType.MEMORY);
      try (ODatabaseSession db = orientDB.open("test", "admin", "admin")) {
        db.createClass("test");
        db.begin();
        ODocument doc = new ODocument("test");
        db.save(doc);
        ((OTransactionInternal) db.getTransaction()).prepareSerializedOperations();
        Iterator<byte[]> res = ((OTransactionInternal) db.getTransaction()).getSerializedOperations();
        assertTrue(res.hasNext());
        byte[] entry = res.next();
        OTransactionDataChange readChange = OTransactionDataChange
            .deserialize(new DataInputStream(new ByteArrayInputStream(entry)));
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
  public void testReApplyFromTransactionData() throws IOException {

    try (OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDB.create("test", ODatabaseType.MEMORY);
      ByteArrayOutputStream backup = new ByteArrayOutputStream();
      try (ODatabaseSession db = orientDB.open("test", "admin", "admin")) {
        db.createClass("test");
        db.begin();
        ODocument doc = new ODocument("test");
        doc.setProperty("field", "value0");
        db.save(doc);
        ODocument doc1 = new ODocument("test");
        doc1.setProperty("field", "value1");
        db.save(doc1);
        db.commit();
        ODatabaseExport export = new ODatabaseExport((ODatabaseDocumentInternal) db, backup, iText -> {
        });
        export.exportDatabase();
        export.close();
      }
      orientDB.create("test1", ODatabaseType.MEMORY);
      try (ODatabaseSession db = orientDB.open("test1", "admin", "admin")) {
        ODatabaseImport imp = new ODatabaseImport((ODatabaseDocumentInternal) db, new ByteArrayInputStream(backup.toByteArray()),
            iText -> {
            });
        imp.importDatabase();
        imp.close();
      }

      OTransactionData data = new OTransactionData(new OTransactionId(Optional.empty(), 1, 2));
      try (ODatabaseSession db = orientDB.open("test", "admin", "admin")) {
        db.begin();
        ODocument doc = new ODocument("test");
        doc.setProperty("field", "value2");
        db.save(doc);
        db.command("update test set field='value3' where field='value1'").close();
        db.command("delete from test where field='value0'").close();

        ((OTransactionInternal) db.getTransaction()).prepareSerializedOperations();
        Iterator<byte[]> res = ((OTransactionInternal) db.getTransaction()).getSerializedOperations();
        while (res.hasNext()) {
          data.addRecord(res.next());
        }
        db.commit();
      }
      try (ODatabaseSession db = orientDB.open("test1", "admin", "admin")) {
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

  }

}
