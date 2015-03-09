package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/9/2015
 *
 * Events:
 * 0. Create new document with value 2L
 * 1. Node 0 starts a transaction, and loads document with version 0 and value 2
 * 2. Node 1 starts a transaction, loads the document with version 0 and value 2, updates it to value 4, and commit(v1)
 * 3*. Node 1 starts a transaction, loads the document with version 1 and value 4, updates it to value 5, and commit(v2)
 * 4. Node 0 update his document (v0, 2) to value 5, and commit.
 * 5. An exception SHOULD be raised, even though the value is the same, because the versions aren't.
 *
 * Note: event 3 is not required, it is just to show the versions don't match nor do they have to be
 * sequential; As long as the content match, the commit will succeed
 */
public abstract class AbstractServerClusterMergeUpdateTest  extends AbstractServerClusterTest {
  private static final String FIELD_NAME = "number";

  private final OPartitionedDatabasePoolFactory poolFactory = new OPartitionedDatabasePoolFactory();

  protected abstract String getDatabaseURL(ServerRun server);

  public String getDatabaseName() {
    return "distributed";
  }

  public void executeTest() throws Exception {
    ODatabaseDocumentTx db0 = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
    try {
      ODatabaseDocumentTx db1 = poolFactory.get(getDatabaseURL(serverInstance.get(1)), "admin", "admin").acquire();
      try {
        executeTest(db0, db1);
      } finally {
        db0.close();
      }
    } finally {
      db0.close();
    }
  }

  private void executeTest(ODatabaseDocumentTx db0, ODatabaseDocumentTx db1) {

    // Event #0: Create new document with value 2L
    ODocument doc = new ODocument("Paper").field(FIELD_NAME, 0L);
    db0.save(doc);
    ORID orid = doc.getIdentity().copy();

    // Event #1: Node 0 starts a transaction, and loads document with version 0 and value 2
    db0.begin();
    ODocument doc0 = db0.load(orid);

    // Event #2: Node 1 starts a transaction, loads the document with
    // version 0 and value 2, updates it to value 4, and commit(v1)
    {
      db1.begin();
      ODocument doc1 = db1.load(orid);
      doc1.field(FIELD_NAME, 4L);
      db1.save(doc1);
      db1.commit();
    }

    // Event #3: Node 1 starts a transaction, loads the document with
    // version 1 and value 4, updates it to value 5, and commit(v2)
    {
      db1.begin();
      ODocument doc1 = db1.load(orid);
      doc1.field(FIELD_NAME, 5L);
      db1.save(doc1);
      db1.commit();
    }

    // Event #4: Node 0 update his document (v0, 2) to value 5, and commit.
    doc0.field(FIELD_NAME, 5L);
    db0.save(doc0);

    boolean anyConflictException = false;
    try {
      db0.commit();

      // Event #5: An exception SHOULD be raised, even though the value is the same, because the versions aren't.
    } catch (RuntimeException ex) {
      if (ex instanceof OConcurrentModificationException) {
        anyConflictException = true;
      } else {
        throw ex;
      }
    }
    Assert.assertTrue(anyConflictException);
  }
}