package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import org.junit.Test;

import java.io.*;
import java.util.Iterator;
import java.util.Optional;

import static org.junit.Assert.*;

public class OTransactionDataReadWriteTest {

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

}
