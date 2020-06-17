package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OAlterSequenceStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OAlterSequenceStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testSetIncrement() {
    String sequenceName = "testSetStart";
    try {
      db.getMetadata()
          .getSequenceLibrary()
          .createSequence(
              sequenceName, OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams());
    } catch (ODatabaseException exc) {
      Assert.assertTrue("Failed to create sequence", false);
    }

    OResultSet result = db.command("alter sequence " + sequenceName + " increment 20");
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals((Object) 20, next.getProperty("increment"));
    result.close();
    OSequence seq = db.getMetadata().getSequenceLibrary().getSequence(sequenceName);
    Assert.assertNotNull(seq);
    try {
      Assert.assertEquals(20, seq.next());
    } catch (ODatabaseException exc) {
      Assert.assertTrue("Failed to call next", false);
    }
  }
}
