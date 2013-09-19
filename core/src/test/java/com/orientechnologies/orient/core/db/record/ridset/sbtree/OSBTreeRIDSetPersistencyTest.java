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
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Test that {@link OSBTreeRIDSet} is saved into the database correctly.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRIDSetPersistencyTest {

  private ODatabaseDocumentTx db;

  @BeforeClass
  public void setUp() throws Exception {
    // OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(Boolean.FALSE);

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
    expected.add(new ORecordId("#77:17"));
    expected.add(new ORecordId("#77:18"));
    expected.add(new ORecordId("#77:19"));
    expected.add(new ORecordId("#77:20"));
    expected.add(new ORecordId("#77:21"));
    expected.add(new ORecordId("#77:22"));

    final OSBTreeRIDSet ridSet = new OSBTreeRIDSet();
    ridSet.addAll(expected);

    final OMVRBTreeRIDSet omvrbTreeRIDSet = new OMVRBTreeRIDSet();
    omvrbTreeRIDSet.addAll(expected);

    final ODocument doc = new ODocument();
    doc.field("ridset", ridSet);
    doc.field("rbtreeset", omvrbTreeRIDSet);
    doc.save();
    final ORID id = doc.getIdentity();

    db.close();
    db.open("admin", "admin");

    final OMVRBTreeRIDSet rb = ((ODocument) db.load(id)).field("rbtreeset");
    Assert.assertEquals(rb.size(), expected.size());
    Assert.assertTrue(rb.containsAll(expected));

    final OSBTreeRIDSet loaded = ((ODocument) db.load(id)).field("ridset");

    Assert.assertEquals(loaded.size(), expected.size());
    Assert.assertTrue(loaded.containsAll(expected));
  }
}
