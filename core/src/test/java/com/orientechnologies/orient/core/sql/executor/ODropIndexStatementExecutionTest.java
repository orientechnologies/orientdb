package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ODropIndexStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:ODropIndexStatementExecutionTest");
    db.create();
  }

  @AfterClass public static void afterClass() {
    db.close();
  }

  @Test public void testPlain() {
    OIndex<?> index = db.getMetadata().getSchema().createClass("testPlain").createProperty("bar", OType.STRING)
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    String indexName = index.getName();

    Assert.assertNotNull(((OIndexManager)db.getMetadata().getIndexManager().reload()).getIndex(indexName));

    OResultSet result = db.command("drop index " + indexName);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop index", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    Assert.assertNull(db.getMetadata().getIndexManager().reload().getIndex(indexName));
  }

  @Test public void testAll() {
    OIndex<?> index = db.getMetadata().getSchema().createClass("testAll").createProperty("baz", OType.STRING)
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    String indexName = index.getName();

    Assert.assertNotNull(db.getMetadata().getIndexManager().reload().getIndex(indexName));

    OResultSet result = db.command("drop index *");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop index", next.getProperty("operation"));
    result.close();

    Assert.assertNull(db.getMetadata().getIndexManager().reload().getIndex(indexName));
    Assert.assertTrue(db.getMetadata().getIndexManager().getIndexes().isEmpty());
  }

  @Test public void testWrongName() {

    String indexName = "nonexistingindex";
    Assert.assertNull(db.getMetadata().getIndexManager().reload().getIndex(indexName));

    try {
      OResultSet result = db.command("drop index " + indexName);
      Assert.fail();
    } catch (OCommandExecutionException e) {
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
