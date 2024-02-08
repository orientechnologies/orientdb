package com.orientechnologies.orient.server.query;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import com.orientechnologies.orient.server.OClientConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/** Created by tglman on 03/01/17. */
public class RemoteQuerySupportTest extends BaseServerMemoryDatabase {

  private int oldPageSize;

  public void beforeTest() {
    super.beforeTest();
    db.createClass("Some");
    oldPageSize = QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(10);
  }

  @Test
  public void testQuery() {
    for (int i = 0; i < 150; i++) {
      ODocument doc = new ODocument("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
    }
    OResultSet res = db.query("select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      OResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testCommandSelect() {
    for (int i = 0; i < 150; i++) {
      ODocument doc = new ODocument("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
    }
    OResultSet res = db.command("select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      OResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testCommandInsertWithPageOverflow() {
    for (int i = 0; i < 150; i++) {
      ODocument doc = new ODocument("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
    }

    OResultSet res = db.command("insert into V from select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      OResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test(expected = ODatabaseException.class)
  public void testQueryKilledSession() {
    for (int i = 0; i < 150; i++) {
      ODocument doc = new ODocument("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
    }
    OResultSet res = db.query("select from Some");

    for (OClientConnection conn : server.getClientConnectionManager().getConnections()) {
      conn.close();
    }
    db.activateOnCurrentThread();

    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      OResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testQueryEmbedded() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    doc.setProperty("emb", emb, OType.EMBEDDED);
    db.save(doc);
    OResultSet res = db.query("select emb from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("emb"));
    assertEquals(((OResult) item.getProperty("emb")).getProperty("one"), "value");
  }

  @Test
  public void testQueryDoubleEmbedded() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb1 = new ODocument();
    emb1.setProperty("two", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    emb.setProperty("secEmb", emb1, OType.EMBEDDED);

    doc.setProperty("emb", emb, OType.EMBEDDED);
    db.save(doc);
    OResultSet res = db.query("select emb from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("emb"));
    OResult resEmb = item.getProperty("emb");
    assertEquals(resEmb.getProperty("one"), "value");
    assertEquals(((OResult) resEmb.getProperty("secEmb")).getProperty("two"), "value");
  }

  @Test
  public void testQueryEmbeddedList() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    List<Object> list = new ArrayList<>();
    list.add(emb);
    doc.setProperty("list", list, OType.EMBEDDEDLIST);
    db.save(doc);
    OResultSet res = db.query("select list from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("list"));
    assertEquals(((List<OResult>) item.getProperty("list")).size(), 1);
    assertEquals(((List<OResult>) item.getProperty("list")).get(0).getProperty("one"), "value");
  }

  @Test
  public void testQueryEmbeddedSet() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    Set<ODocument> set = new HashSet<>();
    set.add(emb);
    doc.setProperty("set", set, OType.EMBEDDEDSET);
    db.save(doc);
    OResultSet res = db.query("select set from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("set"));
    assertEquals(((Set<OResult>) item.getProperty("set")).size(), 1);
    assertEquals(
        ((Set<OResult>) item.getProperty("set")).iterator().next().getProperty("one"), "value");
  }

  @Test
  public void testQueryEmbeddedMap() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    Map<String, ODocument> map = new HashMap<>();
    map.put("key", emb);
    doc.setProperty("map", map, OType.EMBEDDEDMAP);
    db.save(doc);
    OResultSet res = db.query("select map from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("map"));
    assertEquals(((Map<String, OResult>) item.getProperty("map")).size(), 1);
    assertEquals(
        ((Map<String, OResult>) item.getProperty("map")).get("key").getProperty("one"), "value");
  }

  @Test
  public void testCommandWithTX() {

    db.begin();

    db.command("insert into Some set prop = 'value'");

    ORecord record;

    try (OResultSet resultSet = db.command("insert into Some set prop = 'value'")) {
      record = resultSet.next().getRecord().get();
    }

    db.commit();

    Assert.assertTrue(record.getIdentity().isPersistent());
  }

  @Test(expected = OSerializationException.class)
  public void testBrokenParameter() {
    try {
      db.query("select from Some where prop= ?", new Object()).close();
    } catch (RuntimeException e) {
      // should be possible to run a query after without getting the server stuck
      db.query("select from Some where prop= ?", new ORecordId(10, 10)).close();
      throw e;
    }
  }

  @Test
  public void testScriptWithRidbags() {
    db.command("create class testScriptWithRidbagsV extends V");
    db.command("create class testScriptWithRidbagsE extends E");
    db.command("create vertex testScriptWithRidbagsV set name = 'a'");
    db.command("create vertex testScriptWithRidbagsV set name = 'b'");

    db.command(
        "create edge testScriptWithRidbagsE from (select from testScriptWithRidbagsV where name ="
            + " 'a') TO (select from testScriptWithRidbagsV where name = 'b');");

    String script = "";
    script += "BEGIN;";
    script += "LET q1 = SELECT * FROM testScriptWithRidbagsV WHERE name = 'a';";
    script += "LET q2 = SELECT * FROM testScriptWithRidbagsV WHERE name = 'b';";
    script += "COMMIT ;";
    script += "RETURN [$q1,$q2]";

    OResultSet rs = db.execute("sql", script);

    rs.forEachRemaining(x -> System.out.println(x));
    rs.close();
  }

  @Test
  public void testLetOut() {
    db.command("create class letVertex extends V");
    db.command("create class letEdge extends E");
    db.command("create vertex letVertex set name = 'a'");
    db.command("create vertex letVertex set name = 'b'");
    db.command(
        "create edge letEdge from (select from letVertex where name = 'a') TO (select from"
            + " letVertex where name = 'b');");

    OResultSet rs =
        db.query("select $someNode.in('letEdge') from letVertex LET $someNode =out('letEdge');");
    assertEquals(rs.stream().count(), 2);
  }

  public void afterTest() {
    super.afterTest();
    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(oldPageSize);
  }
}
