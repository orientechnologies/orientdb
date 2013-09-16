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
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Test that {@link OSBTreeRIDSet} is saved into the database correctly.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRIDSetPersistencyTest {

  private ODatabaseDocumentTx db;

  @BeforeClass
  public void setUp() throws Exception {
    db = new ODatabaseDocumentTx("plocal:target/testdb/OSBTreeRIDSetTest");
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

    final String fileName = ridSet.getFileName();
    final long rootIndex = ridSet.getRootIndex();

    db.close();
    db.open("admin", "admin");

    final OSBTreeRIDSet loaded = new OSBTreeRIDSet(fileName, rootIndex);
    Assert.assertEquals(loaded.size(), expected.size());
    Assert.assertTrue(loaded.containsAll(expected));
  }
}
