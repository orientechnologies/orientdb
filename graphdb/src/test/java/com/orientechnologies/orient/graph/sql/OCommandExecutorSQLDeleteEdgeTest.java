package com.orientechnologies.orient.graph.sql;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OCommandExecutorSQLDeleteEdgeTest {

  private ODatabaseDocumentTx db;
  private ORID                folderId1;
  private ORID                folderId2;
  private ORID                userId1;
  private ORID                userId2;

  @BeforeClass
  public void init() throws Exception {
    db = Orient.instance().getDatabaseFactory().createDatabase("graph", "local:target/OCommandExecutorSQLDeleteEdgeTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    final OSchema schema = db.getMetadata().getSchema();
    schema.createClass("User", schema.getClass("V"));
    schema.createClass("Folder", schema.getClass("V"));
    schema.createClass("CanAccess", schema.getClass("E"));
  }

  @AfterClass
  public void tearDown() throws Exception {
    db.drop();
  }

  @BeforeMethod
  public void setUp() throws Exception {
    db.getMetadata().getSchema().getClass("User").truncate();
    db.getMetadata().getSchema().getClass("Folder").truncate();
    db.getMetadata().getSchema().getClass("CanAccess").truncate();

    userId1 = new ODocument("User").field("username", "gongolo").save().getIdentity();
    userId2 = new ODocument("User").field("username", "user2").save().getIdentity();
    folderId1 = new ODocument("Folder").field("keyId", "01234567893").save().getIdentity();
    folderId2 = new ODocument("Folder").field("keyId", "01234567894").save().getIdentity();

    db.command(new OCommandSQL("create edge CanAccess from " + userId1 + " to " + folderId1)).execute();
  }

  @Test
  public void testFromSelect() throws Exception {
    final int res = (Integer) db.command(
        new OCommandSQL("delete edge CanAccess from (select from User where username = 'gongolo') to " + folderId1)).execute();
    Assert.assertEquals(res, 1);
    Assert.assertTrue(db.query(new OSQLSynchQuery<Object>("select flatten(out(CanAccess)) from " + userId1)).isEmpty());
  }

  @Test
  public void testFromSelectToSelect() throws Exception {
    final int res = (Integer) db
        .command(
            new OCommandSQL(
                "delete edge CanAccess from ( select from User where username = 'gongolo' ) to ( select from Folder where keyId = '01234567893' )"))
        .execute();
    Assert.assertEquals(res, 1);
    Assert.assertTrue(db.query(new OSQLSynchQuery<Object>("select flatten(out(CanAccess)) from " + userId1)).isEmpty());
  }
}
