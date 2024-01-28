package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ODropSequenceStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testPlain() {
    String name = "testPlain";
    try {
      db.getMetadata()
          .getSequenceLibrary()
          .createSequence(name, OSequence.SEQUENCE_TYPE.CACHED, new OSequence.CreateParams());
    } catch (ODatabaseException exc) {
      Assert.assertTrue("Creating sequence failed", false);
    }

    Assert.assertNotNull(db.getMetadata().getSequenceLibrary().getSequence(name));
    OResultSet result = db.command("drop sequence " + name);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop sequence", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    Assert.assertNull(db.getMetadata().getSequenceLibrary().getSequence(name));
  }

  @Test
  public void testNonExisting() {
    String name = "testNonExisting";
    OSequenceLibrary lib = db.getMetadata().getSequenceLibrary();
    Assert.assertNull(lib.getSequence(name));
    try {
      OResultSet result = db.command("drop sequence " + name);
      Assert.fail();
    } catch (OCommandExecutionException ex1) {

    } catch (Exception ex1) {
      Assert.fail();
    }
  }

  @Test
  public void testNonExistingWithIfExists() {
    String name = "testNonExistingWithIfExists";
    OSequenceLibrary lib = db.getMetadata().getSequenceLibrary();
    Assert.assertNull(lib.getSequence(name));

    OResultSet result = db.command("drop sequence " + name + " if exists");
    Assert.assertFalse(result.hasNext());

    try {
      db.getMetadata()
          .getSequenceLibrary()
          .createSequence(name, OSequence.SEQUENCE_TYPE.CACHED, new OSequence.CreateParams());
    } catch (ODatabaseException exc) {
      Assert.assertTrue("Creating sequence failed", false);
    }

    Assert.assertNotNull(db.getMetadata().getSequenceLibrary().getSequence(name));
    result = db.command("drop sequence " + name + " if exists");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop sequence", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    Assert.assertNull(db.getMetadata().getSequenceLibrary().getSequence(name));
  }
}
