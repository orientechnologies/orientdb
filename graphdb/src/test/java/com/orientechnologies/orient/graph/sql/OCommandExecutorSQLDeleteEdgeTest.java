package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
@RunWith(JUnit4.class)
public class OCommandExecutorSQLDeleteEdgeTest {

  private static ODatabaseDocumentTx db;
  private static ORID                folderId1;
  private static ORID                userId1;
  private List<OIdentifiable>        edges;

  @BeforeClass
  public static void init() throws Exception {
    db = new ODatabaseDocumentTx("memory:" + OCommandExecutorSQLDeleteEdgeTest.class.getSimpleName());
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
  public static void tearDown() throws Exception {
    db.drop();

    db = null;
    folderId1 = null;
    userId1 = null;
  }

  @Before
  public void setUp() throws Exception {
    db.close();
    db.open("admin", "admin");

    db.getMetadata().getSchema().getClass("User").truncate();
    db.getMetadata().getSchema().getClass("Folder").truncate();
    db.getMetadata().getSchema().getClass("CanAccess").truncate();

    userId1 = new ODocument("User").field("username", "gongolo").save().getIdentity();
    ORID userId2 = new ODocument("User").field("username", "user2").save().getIdentity();
    folderId1 = new ODocument("Folder").field("keyId", "01234567893").save().getIdentity();
    ORID folderId2 = new ODocument("Folder").field("keyId", "01234567894").save().getIdentity();

    edges = db.command(new OCommandSQL("create edge CanAccess from " + userId1 + " to " + folderId1)).execute();
  }

  @Test
  public void testFromSelect() throws Exception {
    final int res = (Integer) db
        .command(new OCommandSQL("delete edge CanAccess from (select from User where username = 'gongolo') to " + folderId1))
        .execute();
    Assert.assertEquals(res, 1);
    Assert.assertTrue(db.query(new OSQLSynchQuery<Object>("select flatten(out(CanAccess)) from " + userId1)).isEmpty());
  }

  @Test
  public void testFromSelectToSelect() throws Exception {
    final int res = (Integer) db
        .command(new OCommandSQL(
            "delete edge CanAccess from ( select from User where username = 'gongolo' ) to ( select from Folder where keyId = '01234567893' )"))
        .execute();
    Assert.assertEquals(res, 1);
    Assert.assertTrue(db.query(new OSQLSynchQuery<Object>("select flatten(out(CanAccess)) from " + userId1)).isEmpty());
  }

  @Test
  public void testDeleteByRID() throws Exception {
    final int res = (Integer) db.command(new OCommandSQL("delete edge [" + edges.get(0).getIdentity() + "]")).execute();
    Assert.assertEquals(res, 1);
  }

  @Test
  public void testDeleteEdgeWithVertexRid() throws Exception {
    List<ODocument> vertexes = db.command(new OCommandSQL("select from v limit 1")).execute();
    try {
      final int res = (Integer) db.command(new OCommandSQL("delete edge [" + vertexes.get(0).getIdentity() + "]")).execute();
      Assert.fail("Error on deleting an edge with a rid of a vertex");
    } catch (Exception e) {
      // OK
    }
  }

  @Test
  public void testDeleteEdgeBatch() throws Exception {
    // for issue #4622

    for (int i = 0; i < 100; i++) {
      db.command(new OCommandSQL("create vertex User set name = 'foo" + i + "'")).execute();
      db.command(new OCommandSQL("create edge CanAccess from (select from User where name = 'foo" + i + "') to " + folderId1))
          .execute();
    }

    final int res = (Integer) db.command(new OCommandSQL("delete edge CanAccess batch 5")).execute();

    List<?> result = db.query(new OSQLSynchQuery("select expand( in('CanAccess') ) from " + folderId1));
    Assert.assertEquals(result.size(), 0);

  }
}
