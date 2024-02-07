package com.orientechnologies.orient.core.sql.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
@RunWith(JUnit4.class)
public class OCommandExecutorSQLDeleteEdgeTest extends BaseMemoryDatabase {

  private static ORID folderId1;
  private static ORID userId1;
  private List<OIdentifiable> edges;

  public void beforeTest() {
    super.beforeTest();
    final OSchema schema = db.getMetadata().getSchema();
    schema.createClass("User", schema.getClass("V"));
    schema.createClass("Folder", schema.getClass("V"));
    schema.createClass("CanAccess", schema.getClass("E"));

    userId1 = new ODocument("User").field("username", "gongolo").save().getIdentity();
    new ODocument("User").field("username", "user2").save().getIdentity();
    folderId1 = new ODocument("Folder").field("keyId", "01234567893").save().getIdentity();
    new ODocument("Folder").field("keyId", "01234567894").save().getIdentity();

    edges =
        db.command("create edge CanAccess from " + userId1 + " to " + folderId1).stream()
            .map((x) -> x.getIdentity().get())
            .collect(Collectors.toList());
  }

  @Test
  public void testFromSelect() throws Exception {
    OResultSet res =
        db.command(
            "delete edge CanAccess from (select from User where username = 'gongolo') to "
                + folderId1);
    Assert.assertEquals((long) res.next().getProperty("count"), 1);
    Assert.assertFalse(db.query("select expand(out(CanAccess)) from " + userId1).hasNext());
  }

  @Test
  public void testFromSelectToSelect() throws Exception {
    OResultSet res =
        db.command(
            "delete edge CanAccess from ( select from User where username = 'gongolo' ) to ( select"
                + " from Folder where keyId = '01234567893' )");
    assertEquals((long) res.next().getProperty("count"), 1);
    assertFalse(db.query("select expand(out(CanAccess)) from " + userId1).hasNext());
  }

  @Test
  public void testDeleteByRID() throws Exception {
    OResultSet result = db.command("delete edge [" + edges.get(0).getIdentity() + "]");
    assertEquals((long) result.next().getProperty("count"), 1L);
  }

  @Test
  public void testDeleteEdgeWithVertexRid() throws Exception {
    OResultSet vertexes = db.command("select from v limit 1");
    try {
      db.command("delete edge [" + vertexes.next().getIdentity().get() + "]").close();
      Assert.fail("Error on deleting an edge with a rid of a vertex");
    } catch (Exception e) {
      // OK
    }
  }

  @Test
  public void testDeleteEdgeBatch() throws Exception {
    // for issue #4622

    for (int i = 0; i < 100; i++) {
      db.command("create vertex User set name = 'foo" + i + "'").close();
      db.command(
              "create edge CanAccess from (select from User where name = 'foo"
                  + i
                  + "') to "
                  + folderId1)
          .close();
    }

    db.command("delete edge CanAccess batch 5").close();

    OResultSet result = db.query("select expand( in('CanAccess') ) from " + folderId1);
    assertEquals(result.stream().count(), 0);
  }
}
