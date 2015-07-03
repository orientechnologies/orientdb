package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class OMatchStatementExecutionTest {
  private static String      DB_STORAGE = "memory";
  private static String      DB_NAME    = "OMatchStatementExecutionTest";

  static ODatabaseDocumentTx db;

  @BeforeClass
  public static void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();
    getProfilerInstance().startRecording();

    db.command(new OCommandSQL("CREATE class V")).execute();
    db.command(new OCommandSQL("CREATE class E")).execute();
    db.command(new OCommandSQL("CREATE class Person extends V")).execute();
    db.command(new OCommandSQL("CREATE class Friend extends E")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n1'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n2'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n3'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n4'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n5'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n6'")).execute();

    String[][] friendList = new String[][] { { "n1", "n2" }, { "n1", "n3" }, { "n2", "n4" }, { "n4", "n5" }, { "n4", "n6" } };

    for (String[] pair : friendList) {
      db.command(
          new OCommandSQL("CREATE EDGE Friend from (select from Person where name = ?) to (select from Person where name = ?)"))
          .execute(pair[0], pair[1]);
    }

    db.command(new OCommandSQL("CREATE class MathOp extends V")).execute();
    db.command(new OCommandSQL("CREATE VERTEX MathOp set a = 1, b = 3, c = 2")).execute();
    db.command(new OCommandSQL("CREATE VERTEX MathOp set a = 5, b = 3, c = 2")).execute();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (db.isClosed()) {
      db.open("admin", "admin");
    }
    // db.command(new OCommandSQL("drop class foo")).execute();
    // db.getMetadata().getSchema().reload();
    db.close();
  }

  @Test
  public void testSimple() throws Exception {
    List<ODocument> qResult = db.command(new OCommandSQL("match {class:Person, as: person} return person")).execute();
    assertEquals(6, qResult.size());
    for (ODocument doc : qResult) {
      assertTrue(doc.fieldNames().length == 1);
      OIdentifiable personId = doc.field("person");
      ODocument person = personId.getRecord();
      String name = person.field("name");
      assertTrue(name.startsWith("n"));
    }
  }

  @Test
  public void testSimpleWhere() throws Exception {
    List<ODocument> qResult = db.command(
        new OCommandSQL("match {class:Person, as: person, where: (name = 'n1' or name = 'n2')} return person")).execute();

    assertEquals(2, qResult.size());
    for (ODocument doc : qResult) {
      assertTrue(doc.fieldNames().length == 1);
      OIdentifiable personId = doc.field("person");
      ODocument person = personId.getRecord();
      String name = person.field("name");
      assertTrue(name.equals("n1") || name.equals("n2"));
    }
  }

  @Test
  public void testCommonFriends() throws Exception {

    List<ODocument> qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return $matches)"))
        .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).field("name"));
  }

  @Test
  public void testFriendsOfFriends() throws Exception {

    List<ODocument> qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend').out('Friend'){as:friend} return $matches)"))
        .execute();
    assertEquals(1, qResult.size());
    assertEquals("n4", qResult.get(0).field("name"));
  }

  @Test
  public void testFriendsWithName() throws Exception {

    List<ODocument> qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1' and 1 + 1 = 2)}.out('Friend'){as:friend, where:(name = 'n2' and 1 + 1 = 2)} return friend)"))
        .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).field("name"));
  }

  private long indexUsages(ODatabaseDocumentTx db) {
    final long oldIndexUsage;
    try {
      oldIndexUsage = getProfilerInstance().getCounter("db." + DB_NAME + ".query.indexUsed");
      return oldIndexUsage == -1 ? 0 : oldIndexUsage;
    } catch (Exception e) {
      fail();
    }
    return -1l;
  }

  private static OProfilerMBean getProfilerInstance() throws Exception {
    return Orient.instance().getProfiler();

  }
}
