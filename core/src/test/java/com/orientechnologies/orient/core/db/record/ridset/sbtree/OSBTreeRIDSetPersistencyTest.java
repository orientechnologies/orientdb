package com.orientechnologies.orient.core.db.record.ridset.sbtree;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Test that {@link OSBTreeRIDSet} is saved into the database correctly.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRIDSetPersistencyTest {

  private ODatabaseDocumentTx db;

  @BeforeClass
  public void setUp() throws Exception {
    db = new ODatabaseDocumentTx("plocal:target/testdb/OSBTreeRIDSetPersistencyTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
    ODatabaseRecordThreadLocal.INSTANCE.set(db);
  }

  @AfterClass
  public void tearDown() throws Exception {
    db.drop();
  }

  @Test
  public void testSaveLoad() throws Exception {
    Set<OIdentifiable> expected = new HashSet<OIdentifiable>(8);

    expected.add(new ORecordId("#77:12"));
    expected.add(new ORecordId("#77:13"));
    expected.add(new ORecordId("#77:14"));
    expected.add(new ORecordId("#77:15"));
    expected.add(new ORecordId("#77:16"));

    final OSBTreeRIDSet ridSet = new OSBTreeRIDSet();
    ridSet.addAll(expected);

    final ODocument doc = new ODocument();
    doc.field("ridset", ridSet);
    doc.save();
    final ORID id = doc.getIdentity();

    db.close();
    db.open("admin", "admin");

    final OSBTreeRIDSet loaded = ((ODocument) db.load(id)).field("ridset");

    Assert.assertEquals(loaded.size(), expected.size());
    Assert.assertTrue(loaded.containsAll(expected));
  }
}
