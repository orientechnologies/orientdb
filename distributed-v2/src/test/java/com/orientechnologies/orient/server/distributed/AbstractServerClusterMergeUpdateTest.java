package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/9/2015
 *     <p>Events: 0. Create new document with value 2L, and version 1 1. Node 0 starts a
 *     transaction, and loads document with version 1 and value 2 2. Node 1 starts a transaction,
 *     loads the document with version 1 and value 2, updates it to value 4, and commit(v2) 3*. Node
 *     1 starts a transaction, loads the document with version 2 and value 4, updates it to value 5,
 *     and commit(v3) 4. Node 0 update his document (v1, 2) to value 5(v2), and commit. 5. An
 *     exception SHOULD be raised, even though the value is the same, because the versions aren't.
 *     <p>Note: event 3 is not required, it is just to show the versions don't match nor do they
 *     have to be sequential; As long as the content match, the commit will succeed
 */
public abstract class AbstractServerClusterMergeUpdateTest extends AbstractServerClusterTest {
  private static final String FIELD_NAME = "number";

  protected abstract String getDatabaseURL(ServerRun server);

  public String getDatabaseName() {
    return getClass().getSimpleName();
  }

  public void executeTest() throws Exception {
    ODatabaseDocument db0 =
        serverInstance
            .get(0)
            .getServerInstance()
            .getContext()
            .open(getDatabaseName(), "admin", "admin");
    try {
      ODatabaseDocument db1 =
          serverInstance
              .get(1)
              .getServerInstance()
              .getContext()
              .open(getDatabaseName(), "admin", "admin");
      try {
        executeTest(db0, db1);
      } finally {
        db1.activateOnCurrentThread();
        db1.close();
      }
    } finally {
      db0.activateOnCurrentThread();
      db0.close();
    }
  }

  private void executeTest(ODatabaseDocument db0, ODatabaseDocument db1) {

    db0.activateOnCurrentThread();

    // Event #0: Create new document with value 2L, and version 1
    ODocument doc = new ODocument("Paper").field(FIELD_NAME, 2L);
    db0.save(doc);
    ORID orid = doc.getIdentity().copy();

    // Event #1: Node 0 starts a transaction, and loads document with version 1 and value 2
    db0.begin();
    ODocument doc0 = db0.load(orid);

    // Event #2: Node 1 starts a transaction, loads the document with
    // version 1 and value 2, updates it to value 4, and commit(v2)
    {
      db1.activateOnCurrentThread();
      db1.begin();
      ODocument doc1 = db1.load(orid);
      doc1.field(FIELD_NAME, 4L);
      db1.save(doc1);
      db1.commit();
    }

    // Event #3: Node 1 starts a transaction, loads the document with
    // version 2 and value 4, updates it to value 5, and commit(v3)
    {
      db1.begin();
      ODocument doc1 = db1.load(orid);
      doc1.field(FIELD_NAME, 5L);
      db1.save(doc1);
      db1.commit();
    }

    // Event #4: Node 0 update his document (v1, 2) to value 5(v2), and commit.
    db0.activateOnCurrentThread();
    doc0.field(FIELD_NAME, 5L);
    db0.save(doc0);

    boolean anyConflictException = false;
    try {
      db0.commit();

      // Event #5: An exception SHOULD be raised, even though the value is the same, because the
      // versions aren't.
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
