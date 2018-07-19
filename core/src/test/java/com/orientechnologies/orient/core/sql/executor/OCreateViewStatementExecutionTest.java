package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import org.junit.*;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCreateViewStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateViewStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  @Ignore
  public void testPlain() {
    String className = "testPlain";
    OResultSet result = db.command("create view " + className + "  FROM (SELECT FROM V)");
    OSchema schema = db.getMetadata().getSchema();
    OView view = schema.getView(className);
    Assert.assertNotNull(view);
    Assert.assertEquals(className, view.getName());
    result.close();
  }

}
