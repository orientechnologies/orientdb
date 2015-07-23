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

    db.command(new OCommandSQL("CREATE class Employee extends V")).execute();
    db.command(new OCommandSQL("CREATE class Department extends V")).execute();
    db.command(new OCommandSQL("CREATE class ParentDepartment extends E")).execute();
    db.command(new OCommandSQL("CREATE class WorksAt extends E")).execute();
    db.command(new OCommandSQL("CREATE class ManagerOf extends E")).execute();

    int[][] deptHierarchy = new int[10][];
    deptHierarchy[0] = new int[] { 1, 2 };
    deptHierarchy[1] = new int[] { 3, 4 };
    deptHierarchy[2] = new int[] { 5, 6 };
    deptHierarchy[3] = new int[] { 7, 8 };
    deptHierarchy[4] = new int[] {};
    deptHierarchy[5] = new int[] {};
    deptHierarchy[6] = new int[] {};
    deptHierarchy[7] = new int[] { 9 };
    deptHierarchy[8] = new int[] {};
    deptHierarchy[9] = new int[] {};

    String[] deptManagers = { "a", "b", "d", null, null, null, null, "c", null, null };

    String[][] employees = new String[10][];
    employees[0] = new String[] { "p1" };
    employees[1] = new String[] { "p2", "p3" };
    employees[2] = new String[] { "p4", "p5" };
    employees[3] = new String[] { "p6" };
    employees[4] = new String[] { "p7" };
    employees[5] = new String[] { "p8" };
    employees[6] = new String[] { "p9" };
    employees[7] = new String[] { "p10" };
    employees[8] = new String[] { "p11" };
    employees[9] = new String[] { "p12", "p13" };

    // ______ [manager] department _______
    // _____ (employees in department)____
    // ___________________________________
    // ___________________________________
    // ____________[a]0___________________
    // _____________(p1)__________________
    // _____________/___\_________________
    // ____________/_____\________________
    // ___________/_______\_______________
    // _______[b]1_________2[d]___________
    // ______(p2, p3)_____(p4, p5)________
    // _________/_\_________/_\___________
    // ________3___4_______5___6__________
    // ______(p6)_(p7)___(p8)__(p9)_______
    // ______/__\_________________________
    // __[c]7_____8[]_____________________
    // __(p10)___(p11)____________________
    // ___/_______________________________
    // __9[]______________________________
    // (p12, p13)_________________________
    //
    // short description:
    // Department 0 is the company itself, "a" is the CEO
    // p10 works at department 7, his manager is "c"
    // p12 works at department 9, this department has no direct manager, so p12's manager is c (the upper manager)

    for (int i = 0; i < deptHierarchy.length; i++) {
      db.command(new OCommandSQL("CREATE VERTEX Department set name = 'department" + i + "' ")).execute();
    }

    for (int parent = 0; parent < deptHierarchy.length; parent++) {
      int[] children = deptHierarchy[parent];
      for (int child : children) {
        db.command(
            new OCommandSQL("CREATE EDGE ParentDepartment from (select from Department where name = 'department" + child
                + "') to (select from Department where name = 'department" + parent + "') ")).execute();
      }
    }

    for (int dept = 0; dept < deptManagers.length; dept++) {
      String manager = deptManagers[dept];
      if (manager != null) {
        db.command(new OCommandSQL("CREATE Vertex Employee set name = '" + manager + "' ")).execute();

        db.command(
            new OCommandSQL("CREATE EDGE ManagerOf from (select from Employee where name = '" + manager + ""
                + "') to (select from Department where name = 'department" + dept + "') ")).execute();
      }
    }

    for (int dept = 0; dept < employees.length; dept++) {
      String[] employeesForDept = employees[dept];
      for (String employee : employeesForDept) {
        db.command(new OCommandSQL("CREATE Vertex Employee set name = '" + employee + "' ")).execute();

        db.command(
            new OCommandSQL("CREATE EDGE WorksAt from (select from Employee where name = '" + employee + ""
                + "') to (select from Department where name = 'department" + dept + "') ")).execute();
      }
    }

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

  @Test
  public void testWhile() throws Exception {

    List<ODocument> qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 1)} return friend)"))
        .execute();
    assertEquals(3, qResult.size());

    qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 2), where: ($depth=1) } return friend)"))
        .execute();
    assertEquals(2, qResult.size());

    qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 4), where: ($depth=1) } return friend)"))
        .execute();
    assertEquals(2, qResult.size());

    qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend)"))
        .execute();
    assertEquals(6, qResult.size());

  }

  @Test
  public void testMaxDepth() throws Exception {
    List<ODocument> qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth=1) } return friend)"))
        .execute();
    assertEquals(2, qResult.size());

    qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1 } return friend)"))
        .execute();
    assertEquals(3, qResult.size());

    qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 0 } return friend)"))
        .execute();
    assertEquals(1, qResult.size());

    qResult = db
        .command(
            new OCommandSQL(
                "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth > 0) } return friend)"))
        .execute();
    assertEquals(2, qResult.size());

  }

  @Test
  public void testOrgChart() {
    assertEquals("c", getManager("p10").field("name"));
    assertEquals("c", getManager("p12").field("name"));
    assertEquals("b", getManager("p6").field("name"));
  }

  private ODocument getManager(String personName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {class:Employee, where: (name = '" + personName + "')}");
    query.append("  .out('WorksAt')");
    query.append("  .out('ParentDepartment'){");
    query.append("      while: (in('ManagerOf').size() == 0),");
    query.append("      where: (in('ManagerOf').size() > 0)");
    query.append("  }");
    query.append("  .in('ManagerOf'){as: manager}");
    query.append("  return manager");
    query.append(")");

    System.out.println(query);

    List<OIdentifiable> qResult = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, qResult.size());
    return qResult.get(0).getRecord();
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
