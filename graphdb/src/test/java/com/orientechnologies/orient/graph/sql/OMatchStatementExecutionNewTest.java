package com.orientechnologies.orient.graph.sql;

import static org.junit.Assert.fail;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.MatchPrefetchStep;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OMatchStatementExecutionNewTest {
  private static String DB_STORAGE = "memory";
  private static String DB_NAME = "OMatchStatementExecutionNewTest";

  static ODatabaseDocumentTx db;

  @BeforeClass
  public static void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();
    getProfilerInstance().startRecording();

    db.command(new OCommandSQL("CREATE class Person extends V")).execute();
    db.command(new OCommandSQL("CREATE class Friend extends E")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n1'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n2'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n3'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n4'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n5'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX Person set name = 'n6'")).execute();

    String[][] friendList =
        new String[][] {{"n1", "n2"}, {"n1", "n3"}, {"n2", "n4"}, {"n4", "n5"}, {"n4", "n6"}};

    for (String[] pair : friendList) {
      db.command(
              new OCommandSQL(
                  "CREATE EDGE Friend from (select from Person where name = ?) to (select from Person where name = ?)"))
          .execute(pair[0], pair[1]);
    }

    db.command(new OCommandSQL("CREATE class MathOp extends V")).execute();
    db.command(new OCommandSQL("CREATE VERTEX MathOp set a = 1, b = 3, c = 2")).execute();
    db.command(new OCommandSQL("CREATE VERTEX MathOp set a = 5, b = 3, c = 2")).execute();

    initOrgChart();

    initTriangleTest();

    initEdgeIndexTest();

    initDiamondTest();
  }

  private static void initEdgeIndexTest() {
    db.command(new OCommandSQL("CREATE class IndexedVertex extends V")).execute();
    db.command(new OCommandSQL("CREATE property IndexedVertex.uid INTEGER")).execute();
    db.command(new OCommandSQL("CREATE index IndexedVertex_uid on IndexedVertex (uid) NOTUNIQUE"))
        .execute();

    db.command(new OCommandSQL("CREATE class IndexedEdge extends E")).execute();
    db.command(new OCommandSQL("CREATE property IndexedEdge.out LINK")).execute();
    db.command(new OCommandSQL("CREATE property IndexedEdge.in LINK")).execute();
    db.command(
            new OCommandSQL("CREATE index IndexedEdge_out_in on IndexedEdge (out, in) NOTUNIQUE"))
        .execute();

    int nodes = 1000;
    for (int i = 0; i < nodes; i++) {
      ODocument doc = new ODocument("IndexedVertex");
      doc.field("uid", i);
      doc.save();
    }

    for (int i = 0; i < 100; i++) {
      String cmd =
          "CREATE EDGE IndexedEDGE FROM (SELECT FROM IndexedVertex WHERE uid = 0) TO (SELECT FROM IndexedVertex WHERE uid > "
              + (i * nodes / 100)
              + " and uid <"
              + ((i + 1) * nodes / 100)
              + ")";
      db.command(new OCommandSQL(cmd)).execute();
      //      break;
    }

    //    for (int i = 0; i < 100; i++) {
    //      String cmd =
    //          "CREATE EDGE IndexedEDGE FROM (SELECT FROM IndexedVertex WHERE uid > " + ((i * nodes
    // / 100) + 1) + " and uid < " + (
    //              ((i + 1) * nodes / 100) + 1) + ") TO (SELECT FROM IndexedVertex WHERE uid = 1)";
    //      System.out.println(cmd);
    //      db.command(new OCommandSQL(cmd)).execute();
    //    }

    //    db.query("select expand(out()) from IndexedVertex where uid = 0").stream().forEach(x->
    // System.out.println("x = " + x));
  }

  private static void initOrgChart() {

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
    // __[c]7_____8_______________________
    // __(p10)___(p11)____________________
    // ___/_______________________________
    // __9________________________________
    // (p12, p13)_________________________
    //
    // short description:
    // Department 0 is the company itself, "a" is the CEO
    // p10 works at department 7, his manager is "c"
    // p12 works at department 9, this department has no direct manager, so p12's manager is c (the
    // upper manager)

    db.command(new OCommandSQL("CREATE class Employee extends V")).execute();
    db.command(new OCommandSQL("CREATE class Department extends V")).execute();
    db.command(new OCommandSQL("CREATE class ParentDepartment extends E")).execute();
    db.command(new OCommandSQL("CREATE class WorksAt extends E")).execute();
    db.command(new OCommandSQL("CREATE class ManagerOf extends E")).execute();

    int[][] deptHierarchy = new int[10][];
    deptHierarchy[0] = new int[] {1, 2};
    deptHierarchy[1] = new int[] {3, 4};
    deptHierarchy[2] = new int[] {5, 6};
    deptHierarchy[3] = new int[] {7, 8};
    deptHierarchy[4] = new int[] {};
    deptHierarchy[5] = new int[] {};
    deptHierarchy[6] = new int[] {};
    deptHierarchy[7] = new int[] {9};
    deptHierarchy[8] = new int[] {};
    deptHierarchy[9] = new int[] {};

    String[] deptManagers = {"a", "b", "d", null, null, null, null, "c", null, null};

    String[][] employees = new String[10][];
    employees[0] = new String[] {"p1"};
    employees[1] = new String[] {"p2", "p3"};
    employees[2] = new String[] {"p4", "p5"};
    employees[3] = new String[] {"p6"};
    employees[4] = new String[] {"p7"};
    employees[5] = new String[] {"p8"};
    employees[6] = new String[] {"p9"};
    employees[7] = new String[] {"p10"};
    employees[8] = new String[] {"p11"};
    employees[9] = new String[] {"p12", "p13"};

    for (int i = 0; i < deptHierarchy.length; i++) {
      db.command(new OCommandSQL("CREATE VERTEX Department set name = 'department" + i + "' "))
          .execute();
    }

    for (int parent = 0; parent < deptHierarchy.length; parent++) {
      int[] children = deptHierarchy[parent];
      for (int child : children) {
        db.command(
                new OCommandSQL(
                    "CREATE EDGE ParentDepartment from (select from Department where name = 'department"
                        + child
                        + "') to (select from Department where name = 'department"
                        + parent
                        + "') "))
            .execute();
      }
    }

    for (int dept = 0; dept < deptManagers.length; dept++) {
      String manager = deptManagers[dept];
      if (manager != null) {
        db.command(new OCommandSQL("CREATE Vertex Employee set name = '" + manager + "' "))
            .execute();

        db.command(
                new OCommandSQL(
                    "CREATE EDGE ManagerOf from (select from Employee where name = '"
                        + manager
                        + ""
                        + "') to (select from Department where name = 'department"
                        + dept
                        + "') "))
            .execute();
      }
    }

    for (int dept = 0; dept < employees.length; dept++) {
      String[] employeesForDept = employees[dept];
      for (String employee : employeesForDept) {
        db.command(new OCommandSQL("CREATE Vertex Employee set name = '" + employee + "' "))
            .execute();

        db.command(
                new OCommandSQL(
                    "CREATE EDGE WorksAt from (select from Employee where name = '"
                        + employee
                        + ""
                        + "') to (select from Department where name = 'department"
                        + dept
                        + "') "))
            .execute();
      }
    }
  }

  private static void initTriangleTest() {
    db.command(new OCommandSQL("CREATE class TriangleV extends V")).execute();
    db.command(new OCommandSQL("CREATE property TriangleV.uid INTEGER")).execute();
    db.command(new OCommandSQL("CREATE index TriangleV_uid on TriangleV (uid) UNIQUE_HASH_INDEX"))
        .execute();
    db.command(new OCommandSQL("CREATE class TriangleE extends E")).execute();
    for (int i = 0; i < 10; i++) {
      db.command(new OCommandSQL("CREATE VERTEX TriangleV set uid = ?")).execute(i);
    }
    int[][] edges = {
      {0, 1}, {0, 2}, {1, 2}, {1, 3}, {2, 4}, {3, 4}, {3, 5}, {4, 0}, {4, 7}, {6, 7}, {7, 8},
      {7, 9}, {8, 9}, {9, 1}, {8, 3}, {8, 4}
    };
    for (int[] edge : edges) {
      db.command(
              new OCommandSQL(
                  "CREATE EDGE TriangleE from (select from TriangleV where uid = ?) to (select from TriangleV where uid = ?)"))
          .execute(edge[0], edge[1]);
    }
  }

  private static void initDiamondTest() {
    db.command(new OCommandSQL("CREATE class DiamondV extends V")).execute();
    db.command(new OCommandSQL("CREATE class DiamondE extends E")).execute();
    for (int i = 0; i < 4; i++) {
      db.command(new OCommandSQL("CREATE VERTEX DiamondV set uid = ?")).execute(i);
    }
    int[][] edges = {{0, 1}, {0, 2}, {1, 3}, {2, 3}};
    for (int[] edge : edges) {
      db.command(
              new OCommandSQL(
                  "CREATE EDGE DiamondE from (select from DiamondV where uid = ?) to (select from DiamondV where uid = ?)"))
          .execute(edge[0], edge[1]);
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
    OResultSet qResult = db.query("match {class:Person, as: person} return person");
    printExecutionPlan(qResult);

    for (int i = 0; i < 6; i++) {
      OResult item = qResult.next();
      Assert.assertTrue(item.getPropertyNames().size() == 1);
      OElement person = db.load((ORID) item.getProperty("person"));

      String name = person.getProperty("name");
      Assert.assertTrue(name.startsWith("n"));
    }
    qResult.close();
  }

  @Test
  public void testSimpleWhere() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, as: person, where: (name = 'n1' or name = 'n2')} return person");

    for (int i = 0; i < 2; i++) {
      OResult item = qResult.next();
      Assert.assertTrue(item.getPropertyNames().size() == 1);
      OElement personId = db.load((ORID) item.getProperty("person"));

      ODocument person = personId.getRecord();
      String name = person.field("name");
      Assert.assertTrue(name.equals("n1") || name.equals("n2"));
    }
    qResult.close();
  }

  @Test
  public void testSimpleLimit() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit 1");
    Assert.assertTrue(qResult.hasNext());
    qResult.next();
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testSimpleLimit2() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit -1");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(qResult.hasNext());
      qResult.next();
    }
    qResult.close();
  }

  @Test
  public void testSimpleLimit3() throws Exception {

    OResultSet qResult =
        db.query(
            "match {class:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit 3");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(qResult.hasNext());
      qResult.next();
    }
    qResult.close();
  }

  @Test
  public void testSimpleUnnamedParams() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, as: person, where: (name = ? or name = ?)} return person",
            "n1",
            "n2");

    printExecutionPlan(qResult);
    for (int i = 0; i < 2; i++) {

      OResult item = qResult.next();
      Assert.assertTrue(item.getPropertyNames().size() == 1);
      OElement person = db.load((ORID) item.getProperty("person"));

      String name = person.getProperty("name");
      Assert.assertTrue(name.equals("n1") || name.equals("n2"));
    }
    qResult.close();
  }

  @Test
  public void testCommonFriends() throws Exception {

    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return friend)");
    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriendsPatterns() throws Exception {

    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return $patterns)");
    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testPattens() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return $patterns");
    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals(1, item.getPropertyNames().size());
    Assert.assertEquals("friend", item.getPropertyNames().iterator().next());
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testPaths() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return $paths");
    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals(3, item.getPropertyNames().size());
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testElements() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return $elements");
    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testPathElements() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return $pathElements");
    printExecutionPlan(qResult);
    Set<String> expected = new HashSet<>();
    expected.add("n1");
    expected.add("n2");
    expected.add("n4");
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(qResult.hasNext());
      OResult item = qResult.next();
      expected.remove(item.getProperty("name"));
    }
    Assert.assertFalse(qResult.hasNext());
    Assert.assertTrue(expected.isEmpty());
    qResult.close();
  }

  @Test
  public void testCommonFriendsMatches() throws Exception {

    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return $matches)");
    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriendsArrows() throws Exception {

    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return friend)");
    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriendsArrowsPatterns() throws Exception {

    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return $patterns)");
    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriends2() throws Exception {

    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return friend.name as name");

    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriends2Arrows() throws Exception {

    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return friend.name as name");

    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnMethod() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return friend.name.toUpperCase(Locale.ENGLISH) as name");
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("N2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnMethodArrows() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return friend.name.toUpperCase(Locale.ENGLISH) as name");
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("N2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnExpression() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return friend.name + ' ' +friend.name as name");

    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2 n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnExpressionArrows() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return friend.name + ' ' +friend.name as name");

    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2 n2", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnDefaultAlias() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return friend.name");

    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("friend.name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnDefaultAliasArrows() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return friend.name");

    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n2", item.getProperty("friend.name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testFriendsOfFriends() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend').out('Friend'){as:friend} return $matches)");

    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n4", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testFriendsOfFriendsArrows() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{}-Friend->{as:friend} return $matches)");

    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertEquals("n4", item.getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testFriendsOfFriends2() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1'), as: me}.both('Friend').both('Friend'){as:friend, where: ($matched.me != $currentMatch)} return $matches)");

    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    while (qResult.hasNext()) {
      Assert.assertNotEquals(qResult.next().getProperty("name"), "n1");
    }
    qResult.close();
  }

  @Test
  public void testFriendsOfFriends2Arrows() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1'), as: me}-Friend-{}-Friend-{as:friend, where: ($matched.me != $currentMatch)} return $matches)");

    Assert.assertTrue(qResult.hasNext());
    while (qResult.hasNext()) {
      Assert.assertNotEquals(qResult.next().getProperty("name"), "n1");
    }
    qResult.close();
  }

  @Test
  public void testFriendsWithName() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1' and 1 + 1 = 2)}.out('Friend'){as:friend, where:(name = 'n2' and 1 + 1 = 2)} return friend)");

    Assert.assertTrue(qResult.hasNext());
    Assert.assertEquals("n2", qResult.next().getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testFriendsWithNameArrows() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1' and 1 + 1 = 2)}-Friend->{as:friend, where:(name = 'n2' and 1 + 1 = 2)} return friend)");
    Assert.assertTrue(qResult.hasNext());
    Assert.assertEquals("n2", qResult.next().getProperty("name"));
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testWhile() throws Exception {

    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 1)} return friend)");
    Assert.assertEquals(3, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 2), where: ($depth=1) } return friend)");
    Assert.assertEquals(2, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 4), where: ($depth=1) } return friend)");
    Assert.assertEquals(2, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend)");
    Assert.assertEquals(6, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend limit 3)");
    Assert.assertEquals(3, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend) limit 3");
    Assert.assertEquals(3, size(qResult));
    qResult.close();
  }

  private int size(OResultSet qResult) {
    int result = 0;
    while (qResult.hasNext()) {
      result++;
      qResult.next();
    }
    return result;
  }

  @Test
  public void testWhileArrows() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 1)} return friend)");
    Assert.assertEquals(3, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 2), where: ($depth=1) } return friend)");
    Assert.assertEquals(2, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 4), where: ($depth=1) } return friend)");
    Assert.assertEquals(2, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, while: (true) } return friend)");
    Assert.assertEquals(6, size(qResult));
    qResult.close();
  }

  @Test
  public void testMaxDepth() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth=1) } return friend)");
    Assert.assertEquals(2, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1 } return friend)");
    Assert.assertEquals(3, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 0 } return friend)");
    Assert.assertEquals(1, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth > 0) } return friend)");
    Assert.assertEquals(2, size(qResult));
    qResult.close();
  }

  @Test
  public void testMaxDepthArrow() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1, where: ($depth=1) } return friend)");
    Assert.assertEquals(2, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1 } return friend)");
    Assert.assertEquals(3, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 0 } return friend)");
    Assert.assertEquals(1, size(qResult));
    qResult.close();

    qResult =
        db.query(
            "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1, where: ($depth > 0) } return friend)");
    Assert.assertEquals(2, size(qResult));
    qResult.close();
  }

  @Test
  public void testManager() {
    // the manager of a person is the manager of the department that person belongs to.
    // if that department does not have a direct manager, climb up the hierarchy until you find one
    Assert.assertEquals("c", getManager("p10").field("name"));
    Assert.assertEquals("c", getManager("p12").field("name"));
    Assert.assertEquals("b", getManager("p6").field("name"));
    Assert.assertEquals("b", getManager("p11").field("name"));

    Assert.assertEquals("c", getManagerArrows("p10").field("name"));
    Assert.assertEquals("c", getManagerArrows("p12").field("name"));
    Assert.assertEquals("b", getManagerArrows("p6").field("name"));
    Assert.assertEquals("b", getManagerArrows("p11").field("name"));
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

    OResultSet qResult = db.query(query.toString());
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
    return item.getElement().get().getRecord();
  }

  private ODocument getManagerArrows(String personName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {class:Employee, where: (name = '" + personName + "')}");
    query.append("  -WorksAt->{}-ParentDepartment->{");
    query.append("      while: (in('ManagerOf').size() == 0),");
    query.append("      where: (in('ManagerOf').size() > 0)");
    query.append("  }<-ManagerOf-{as: manager}");
    query.append("  return manager");
    query.append(")");

    OResultSet qResult = db.query(query.toString());
    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
    return item.getElement().get().getRecord();
  }

  @Test
  public void testManager2() {
    // the manager of a person is the manager of the department that person belongs to.
    // if that department does not have a direct manager, climb up the hierarchy until you find one

    Assert.assertEquals("c", getManager2("p10").getProperty("name"));
    Assert.assertEquals("c", getManager2("p12").getProperty("name"));
    Assert.assertEquals("b", getManager2("p6").getProperty("name"));
    Assert.assertEquals("b", getManager2("p11").getProperty("name"));

    Assert.assertEquals("c", getManager2Arrows("p10").getProperty("name"));
    Assert.assertEquals("c", getManager2Arrows("p12").getProperty("name"));
    Assert.assertEquals("b", getManager2Arrows("p6").getProperty("name"));
    Assert.assertEquals("b", getManager2Arrows("p11").getProperty("name"));
  }

  private OResult getManager2(String personName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {class:Employee, where: (name = '" + personName + "')}");
    query.append("   .( out('WorksAt')");
    query.append("     .out('ParentDepartment'){");
    query.append("       while: (in('ManagerOf').size() == 0),");
    query.append("       where: (in('ManagerOf').size() > 0)");
    query.append("     }");
    query.append("   )");
    query.append("  .in('ManagerOf'){as: manager}");
    query.append("  return manager");
    query.append(")");

    OResultSet qResult = db.query(query.toString());
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
    return item;
  }

  private OResult getManager2Arrows(String personName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {class:Employee, where: (name = '" + personName + "')}");
    query.append("   .( -WorksAt->{}-ParentDepartment->{");
    query.append("       while: (in('ManagerOf').size() == 0),");
    query.append("       where: (in('ManagerOf').size() > 0)");
    query.append("     }");
    query.append("   )<-ManagerOf-{as: manager}");
    query.append("  return manager");
    query.append(")");

    OResultSet qResult = db.query(query.toString());
    Assert.assertTrue(qResult.hasNext());
    OResult item = qResult.next();
    Assert.assertFalse(qResult.hasNext());
    qResult.close();
    return item;
  }

  @Test
  public void testManaged() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    OResultSet managedByA = getManagedBy("a");
    Assert.assertTrue(managedByA.hasNext());
    OResult item = managedByA.next();
    Assert.assertFalse(managedByA.hasNext());
    Assert.assertEquals("p1", item.getProperty("name"));
    managedByA.close();

    OResultSet managedByB = getManagedBy("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(managedByB.hasNext());
      OResult id = managedByB.next();
      String name = id.getProperty("name");
      names.add(name);
    }
    Assert.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private OResultSet getManagedBy(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(managed) from (");
    query.append("  match {class:Employee, where: (name = '" + managerName + "')}");
    query.append("  .out('ManagerOf')");
    query.append("  .in('ParentDepartment'){");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }");
    query.append("  .in('WorksAt'){as: managed}");
    query.append("  return managed");
    query.append(")");

    return db.query(query.toString());
  }

  @Test
  public void testManagedArrows() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    OResultSet managedByA = getManagedByArrows("a");
    Assert.assertTrue(managedByA.hasNext());
    OResult item = managedByA.next();
    Assert.assertFalse(managedByA.hasNext());
    Assert.assertEquals("p1", item.getProperty("name"));
    managedByA.close();
    OResultSet managedByB = getManagedByArrows("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(managedByB.hasNext());
      OResult id = managedByB.next();
      String name = id.getProperty("name");
      names.add(name);
    }
    Assert.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private OResultSet getManagedByArrows(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(managed) from (");
    query.append("  match {class:Employee, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}<-ParentDepartment-{");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return managed");
    query.append(")");

    return db.query(query.toString());
  }

  @Test
  public void testManaged2() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    OResultSet managedByA = getManagedBy2("a");
    Assert.assertTrue(managedByA.hasNext());
    OResult item = managedByA.next();
    Assert.assertFalse(managedByA.hasNext());
    Assert.assertEquals("p1", item.getProperty("name"));
    managedByA.close();
    OResultSet managedByB = getManagedBy2("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(managedByB.hasNext());
      OResult id = managedByB.next();
      String name = id.getProperty("name");
      names.add(name);
    }
    Assert.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private OResultSet getManagedBy2(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(managed) from (");
    query.append("  match {class:Employee, where: (name = '" + managerName + "')}");
    query.append("  .out('ManagerOf')");
    query.append("  .(inE('ParentDepartment').outV()){");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }");
    query.append("  .in('WorksAt'){as: managed}");
    query.append("  return managed");
    query.append(")");

    return db.query(query.toString());
  }

  @Test
  public void testManaged2Arrows() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    OResultSet managedByA = getManagedBy2Arrows("a");
    Assert.assertTrue(managedByA.hasNext());
    OResult item = managedByA.next();
    Assert.assertFalse(managedByA.hasNext());
    Assert.assertEquals("p1", item.getProperty("name"));
    managedByA.close();
    OResultSet managedByB = getManagedBy2Arrows("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(managedByB.hasNext());
      OResult id = managedByB.next();
      String name = id.getProperty("name");
      names.add(name);
    }
    Assert.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private OResultSet getManagedBy2Arrows(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(managed) from (");
    query.append("  match {class:Employee, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}");
    query.append("  .(inE('ParentDepartment').outV()){");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return managed");
    query.append(")");

    return db.query(query.toString());
  }

  @Test
  public void testTriangle1() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("  .out('TriangleE'){as: friend2}");
    query.append("  .out('TriangleE'){as: friend3},");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend3}");
    query.append("return $matches");

    OResultSet result = db.query(query.toString());

    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangle1Arrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append(
        "{class:TriangleV, as: friend1, where: (uid = 0)} -TriangleE-> {as: friend2} -TriangleE-> {as: friend3},");
    query.append("{class:TriangleV, as: friend1} -TriangleE-> {as: friend3}");
    query.append("return $matches");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangle2Old() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){class:TriangleV, as: friend2, where: (uid = 1)}");
    query.append("  .out('TriangleE'){as: friend3},");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend3}");
    query.append("return $matches");

    OResultSet result = db.query(query.toString());
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    OElement friend1 = db.load((ORID) doc.getProperty("friend1"));
    OElement friend2 = db.load((ORID) doc.getProperty("friend2"));
    OElement friend3 = db.load((ORID) doc.getProperty("friend3"));
    Assert.assertEquals(0, friend1.<Object>getProperty("uid"));
    Assert.assertEquals(1, friend2.<Object>getProperty("uid"));
    Assert.assertEquals(2, friend3.<Object>getProperty("uid"));
    result.close();
  }

  @Test
  public void testTriangle2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){class:TriangleV, as: friend2, where: (uid = 1)}");
    query.append("  .out('TriangleE'){as: friend3},");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend3}");
    query.append("return $patterns");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    OElement friend1 = db.load((ORID) doc.getProperty("friend1"));
    OElement friend2 = db.load((ORID) doc.getProperty("friend2"));
    OElement friend3 = db.load((ORID) doc.getProperty("friend3"));
    Assert.assertEquals(0, friend1.<Object>getProperty("uid"));
    Assert.assertEquals(1, friend2.<Object>getProperty("uid"));
    Assert.assertEquals(2, friend3.<Object>getProperty("uid"));
    result.close();
  }

  @Test
  public void testTriangle2Arrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  -TriangleE->{class:TriangleV, as: friend2, where: (uid = 1)}");
    query.append("  -TriangleE->{as: friend3},");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend3}");
    query.append("return $matches");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    OElement friend1 = db.load((ORID) doc.getProperty("friend1"));
    OElement friend2 = db.load((ORID) doc.getProperty("friend2"));
    OElement friend3 = db.load((ORID) doc.getProperty("friend3"));
    Assert.assertEquals(0, friend1.<Object>getProperty("uid"));
    Assert.assertEquals(1, friend2.<Object>getProperty("uid"));
    Assert.assertEquals(2, friend3.<Object>getProperty("uid"));
    result.close();
  }

  @Test
  public void testTriangle3() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend2}");
    query.append("  -TriangleE->{as: friend3, where: (uid = 2)},");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend3}");
    query.append("return $matches");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangle4() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend2, where: (uid = 1)}");
    query.append("  .out('TriangleE'){as: friend3},");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend3}");
    query.append("return $matches");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangle4Arrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend2, where: (uid = 1)}");
    query.append("  -TriangleE->{as: friend3},");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend3}");
    query.append("return $matches");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangleWithEdges4() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  .outE('TriangleE').inV(){as: friend2, where: (uid = 1)}");
    query.append("  .outE('TriangleE').inV(){as: friend3},");
    query.append("{class:TriangleV, as: friend1}");
    query.append("  .outE('TriangleE').inV(){as: friend3}");
    query.append("return $matches");

    OResultSet result = db.query(query.toString());
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCartesianProduct() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where:(uid = 1)},");
    query.append("{class:TriangleV, as: friend2, where:(uid = 2 or uid = 3)}");
    query.append("return $matches");

    OResultSet result = db.query(query.toString());
    printExecutionPlan(result);

    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult doc = result.next();
      OElement friend1 = db.load((ORID) doc.getProperty("friend1"));
      Assert.assertEquals(friend1.<Object>getProperty("uid"), 1);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNoPrefetch() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one}");
    query.append("return $patterns");

    OResultSet result = db.query(query.toString());
    printExecutionPlan(result);

    result
        .getExecutionPlan()
        .ifPresent(
            x ->
                x.getSteps().stream()
                    .filter(y -> y instanceof MatchPrefetchStep)
                    .forEach(prefetchStepFound -> Assert.fail()));

    for (int i = 0; i < 1000; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCartesianProductLimit() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where:(uid = 1)},");
    query.append("{class:TriangleV, as: friend2, where:(uid = 2 or uid = 3)}");
    query.append("return $matches LIMIT 1");

    OResultSet result = db.query(query.toString());

    Assert.assertTrue(result.hasNext());
    OResult d = result.next();
    OElement friend1 = db.load((ORID) d.getProperty("friend1"));
    Assert.assertEquals(friend1.<Object>getProperty("uid"), 1);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testArrayNumber() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0] as foo");

    OResultSet result = db.query(query.toString());

    Assert.assertTrue(result.hasNext());

    OResult doc = result.next();
    Object foo = db.load((ORID) doc.getProperty("foo"));
    Assert.assertNotNull(foo);
    Assert.assertTrue(foo instanceof OVertex);
    result.close();
  }

  @Test
  public void testArraySingleSelectors2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0,1] as foo");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    Object foo = doc.getProperty("foo");
    Assert.assertNotNull(foo);
    Assert.assertTrue(foo instanceof List);
    Assert.assertEquals(2, ((List) foo).size());
    result.close();
  }

  @Test
  public void testArrayRangeSelectors1() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..1] as foo");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());

    Object foo = doc.getProperty("foo");
    Assert.assertNotNull(foo);
    Assert.assertTrue(foo instanceof List);
    Assert.assertEquals(1, ((List) foo).size());
    result.close();
  }

  @Test
  public void testArrayRange2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..2] as foo");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());

    Object foo = doc.getProperty("foo");
    Assert.assertNotNull(foo);
    Assert.assertTrue(foo instanceof List);
    Assert.assertEquals(2, ((List) foo).size());
    result.close();
  }

  @Test
  public void testArrayRange3() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..3] as foo");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());

    Object foo = doc.getProperty("foo");
    Assert.assertNotNull(foo);
    Assert.assertTrue(foo instanceof List);
    Assert.assertEquals(2, ((List) foo).size());
    result.close();
  }

  @Test
  public void testConditionInSquareBrackets() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[uid = 2] as foo");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());

    Object foo = doc.getProperty("foo");
    Assert.assertNotNull(foo);
    Assert.assertTrue(foo instanceof List);
    Assert.assertEquals(1, ((List) foo).size());
    OVertex resultVertex = (OVertex) ((List) foo).get(0);
    Assert.assertEquals(2, resultVertex.<Object>getProperty("uid"));
    result.close();
  }

  @Test
  public void testIndexedEdge() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)}");
    query.append(".out('IndexedEdge'){class:IndexedVertex, as: two, where: (uid = 1)}");
    query.append("return one, two");

    OResultSet result = db.query(query.toString());
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testIndexedEdgeArrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)}");
    query.append("-IndexedEdge->{class:IndexedVertex, as: two, where: (uid = 1)}");
    query.append("return one, two");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testJson() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)} ");
    query.append("return {'name':'foo', 'uuid':one.uid}");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());

    //    ODocument doc = result.get(0);
    //    assertEquals("foo", doc.field("name"));
    //    assertEquals(0, doc.field("uuid"));
    result.close();
  }

  @Test
  public void testJson2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)} ");
    query.append("return {'name':'foo', 'sub': {'uuid':one.uid}}");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    //    ODocument doc = result.get(0);
    //    assertEquals("foo", doc.field("name"));
    //    assertEquals(0, doc.field("sub.uuid"));
    result.close();
  }

  @Test
  public void testJson3() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)} ");
    query.append("return {'name':'foo', 'sub': [{'uuid':one.uid}]}");

    OResultSet result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());
    //    ODocument doc = result.get(0);
    //    assertEquals("foo", doc.field("name"));
    //    assertEquals(0, doc.field("sub[0].uuid"));

    result.close();
  }

  @Test
  public void testUnique() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append(
        "{class:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return DISTINCT one, two");

    OResultSet result = db.query(query.toString());
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertFalse(result.hasNext());

    query = new StringBuilder();
    query.append("match ");
    query.append(
        "{class:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return DISTINCT one.uid, two.uid");

    result.close();

    result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    doc = result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
    //    ODocument doc = result.get(0);
    //    assertEquals("foo", doc.field("name"));
    //    assertEquals(0, doc.field("sub[0].uuid"));
  }

  @Test
  public void testNotUnique() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append(
        "{class:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return one, two");

    OResultSet result = db.query(query.toString());
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult doc = result.next();
    Assert.assertTrue(result.hasNext());
    doc = result.next();
    Assert.assertFalse(result.hasNext());
    result.close();

    query = new StringBuilder();
    query.append("match ");
    query.append(
        "{class:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return one.uid, two.uid");

    result = db.query(query.toString());
    Assert.assertTrue(result.hasNext());
    doc = result.next();
    Assert.assertTrue(result.hasNext());
    doc = result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
    //    ODocument doc = result.get(0);
    //    assertEquals("foo", doc.field("name"));
    //    assertEquals(0, doc.field("sub[0].uuid"));
  }

  @Test
  public void testManagedElements() {
    OResultSet managedByB = getManagedElements("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("b");
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 6; i++) {
      Assert.assertTrue(managedByB.hasNext());
      OResult doc = managedByB.next();
      String name = doc.getProperty("name");
      names.add(name);
    }
    Assert.assertFalse(managedByB.hasNext());
    Assert.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private OResultSet getManagedElements(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("  match {class:Employee, as:boss, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}<-ParentDepartment-{");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return distinct $elements");

    return db.query(query.toString());
  }

  @Test
  public void testManagedPathElements() {
    OResultSet managedByB = getManagedPathElements("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("department1");
    expectedNames.add("department3");
    expectedNames.add("department4");
    expectedNames.add("department8");
    expectedNames.add("b");
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(managedByB.hasNext());
      OResult doc = managedByB.next();
      String name = doc.getProperty("name");
      names.add(name);
    }
    Assert.assertFalse(managedByB.hasNext());
    Assert.assertEquals(expectedNames, names);
    managedByB.close();
  }

  @Test
  public void testOptional() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, as: person} -NonExistingEdge-> {as:b, optional:true} return person, b.name");

    printExecutionPlan(qResult);
    for (int i = 0; i < 6; i++) {
      Assert.assertTrue(qResult.hasNext());
      OResult doc = qResult.next();
      Assert.assertTrue(doc.getPropertyNames().size() == 2);
      OElement person = db.load((ORID) doc.getProperty("person"));

      String name = person.getProperty("name");
      Assert.assertTrue(name.startsWith("n"));
    }
  }

  @Test
  public void testOptional2() throws Exception {
    OResultSet qResult =
        db.query(
            "match {class:Person, as: person} --> {as:b, optional:true, where:(nonExisting = 12)} return person, b.name");

    for (int i = 0; i < 6; i++) {
      Assert.assertTrue(qResult.hasNext());
      OResult doc = qResult.next();
      Assert.assertTrue(doc.getPropertyNames().size() == 2);
      OElement person = db.load((ORID) doc.getProperty("person"));

      String name = person.getProperty("name");
      Assert.assertTrue(name.startsWith("n"));
    }
  }

  @Test
  public void testOptional3() throws Exception {
    OResultSet qResult =
        db.query(
            "select friend.name as name, b from ("
                + "match {class:Person, as:a, where:(name = 'n1' and 1 + 1 = 2)}.out('Friend'){as:friend, where:(name = 'n2' and 1 + 1 = 2)},"
                + "{as:a}.out(){as:b, where:(nonExisting = 12), optional:true},"
                + "{as:friend}.out(){as:b, optional:true}"
                + " return friend, b)");

    printExecutionPlan(qResult);
    Assert.assertTrue(qResult.hasNext());
    OResult doc = qResult.next();
    Assert.assertEquals("n2", doc.getProperty("name"));
    Assert.assertNull(doc.getProperty("b"));
    Assert.assertFalse(qResult.hasNext());
  }

  @Test
  public void testOrderByAsc() {
    db.command(new OCommandSQL("CREATE CLASS testOrderByAsc EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX testOrderByAsc SET name = 'bbb'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testOrderByAsc SET name = 'zzz'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testOrderByAsc SET name = 'aaa'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testOrderByAsc SET name = 'ccc'")).execute();

    String query = "MATCH { class: testOrderByAsc, as:a} RETURN a.name as name order by name asc";

    OResultSet result = db.query(query);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("aaa", result.next().getProperty("name"));
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("bbb", result.next().getProperty("name"));
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("ccc", result.next().getProperty("name"));
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("zzz", result.next().getProperty("name"));
    Assert.assertFalse(result.hasNext());
  }

  @Test
  public void testOrderByDesc() {
    db.command(new OCommandSQL("CREATE CLASS testOrderByDesc EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX testOrderByDesc SET name = 'bbb'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testOrderByDesc SET name = 'zzz'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testOrderByDesc SET name = 'aaa'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testOrderByDesc SET name = 'ccc'")).execute();

    String query = "MATCH { class: testOrderByDesc, as:a} RETURN a.name as name order by name desc";

    OResultSet result = db.query(query);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("zzz", result.next().getProperty("name"));
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("ccc", result.next().getProperty("name"));
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("bbb", result.next().getProperty("name"));
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("aaa", result.next().getProperty("name"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNestedProjections() {
    String clazz = "testNestedProjections";
    db.command(new OCommandSQL("CREATE CLASS " + clazz + " EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'bbb', surname = 'ccc'"))
        .execute();

    String query = "MATCH { class: " + clazz + ", as:a} RETURN a:{name}, 'x' ";

    OResultSet result = db.query(query);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    OResult a = item.getProperty("a");
    Assert.assertEquals("bbb", a.getProperty("name"));
    Assert.assertNull(a.getProperty("surname"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testAggregate() {
    String clazz = "testAggregate";
    db.command(new OCommandSQL("CREATE CLASS " + clazz + " EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', num = 1")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', num = 2")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', num = 3")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'bbb', num = 4")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'bbb', num = 5")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'bbb', num = 6")).execute();

    String query =
        "MATCH { class: "
            + clazz
            + ", as:a} RETURN a.name as a, max(a.num) as maxNum group by a.name order by a.name";

    OResultSet result = db.query(query);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("aaa", item.getProperty("a"));
    Assert.assertEquals(3, (int) item.getProperty("maxNum"));

    Assert.assertTrue(result.hasNext());
    item = result.next();
    Assert.assertEquals("bbb", item.getProperty("a"));
    Assert.assertEquals(6, (int) item.getProperty("maxNum"));

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testOrderByOutOfProjAsc() {
    String clazz = "testOrderByOutOfProjAsc";
    db.command(new OCommandSQL("CREATE CLASS " + clazz + " EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', num = 0, num2 = 1"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', num = 1, num2 = 2"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', num = 2, num2 = 3"))
        .execute();

    String query =
        "MATCH { class: "
            + clazz
            + ", as:a} RETURN a.name as name, a.num as num order by a.num2 asc";

    OResultSet result = db.query(query);
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals("aaa", item.getProperty("name"));
      Assert.assertEquals(i, (int) item.getProperty("num"));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testOrderByOutOfProjDesc() {
    String clazz = "testOrderByOutOfProjDesc";
    db.command(new OCommandSQL("CREATE CLASS " + clazz + " EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', num = 0, num2 = 1"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', num = 1, num2 = 2"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', num = 2, num2 = 3"))
        .execute();

    String query =
        "MATCH { class: "
            + clazz
            + ", as:a} RETURN a.name as name, a.num as num order by a.num2 desc";

    OResultSet result = db.query(query);
    for (int i = 2; i >= 0; i--) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals("aaa", item.getProperty("name"));
      Assert.assertEquals(i, (int) item.getProperty("num"));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUnwind() {
    String clazz = "testUnwind";
    db.command(new OCommandSQL("CREATE CLASS " + clazz + " EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa', coll = [1, 2]"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'bbb', coll = [3, 4]"))
        .execute();

    String query =
        "MATCH { class: " + clazz + ", as:a} RETURN a.name as name, a.coll as num unwind num";

    int sum = 0;
    OResultSet result = db.query(query);
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      sum += (int) item.getProperty("num");
    }

    Assert.assertFalse(result.hasNext());

    result.close();
    Assert.assertEquals(10, sum);
  }

  @Test
  public void testSkip() {
    String clazz = "testSkip";
    db.command(new OCommandSQL("CREATE CLASS " + clazz + " EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'bbb'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'ccc'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'ddd'")).execute();

    String query =
        "MATCH { class: "
            + clazz
            + ", as:a} RETURN a.name as name ORDER BY name ASC skip 1 limit 2";

    OResultSet result = db.query(query);

    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("bbb", item.getProperty("name"));

    Assert.assertTrue(result.hasNext());
    item = result.next();
    Assert.assertEquals("ccc", item.getProperty("name"));

    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testDepthAlias() {
    String clazz = "testDepthAlias";
    db.command(new OCommandSQL("CREATE CLASS " + clazz + " EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'bbb'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'ccc'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'ddd'")).execute();

    db.command(
            new OCommandSQL(
                "CREATE EDGE E FROM (SELECT FROM "
                    + clazz
                    + " WHERE name = 'aaa') TO (SELECT FROM "
                    + clazz
                    + " WHERE name = 'bbb')"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE E FROM (SELECT FROM "
                    + clazz
                    + " WHERE name = 'bbb') TO (SELECT FROM "
                    + clazz
                    + " WHERE name = 'ccc')"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE E FROM (SELECT FROM "
                    + clazz
                    + " WHERE name = 'ccc') TO (SELECT FROM "
                    + clazz
                    + " WHERE name = 'ddd')"))
        .execute();

    String query =
        "MATCH { class: "
            + clazz
            + ", as:a, where:(name = 'aaa')} --> {as:b, while:($depth<10), depthAlias: xy} RETURN a.name as name, b.name as bname, xy";

    OResultSet result = db.query(query);

    int sum = 0;
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Object depth = item.getProperty("xy");
      Assert.assertTrue(depth instanceof Integer);
      Assert.assertEquals("aaa", item.getProperty("name"));
      switch ((int) depth) {
        case 0:
          Assert.assertEquals("aaa", item.getProperty("bname"));
          break;
        case 1:
          Assert.assertEquals("bbb", item.getProperty("bname"));
          break;
        case 2:
          Assert.assertEquals("ccc", item.getProperty("bname"));
          break;
        case 3:
          Assert.assertEquals("ddd", item.getProperty("bname"));
          break;
        default:
          Assert.fail();
      }
      sum += (int) depth;
    }
    Assert.assertEquals(sum, 6);
    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testPathAlias() {
    String clazz = "testPathAlias";
    db.command(new OCommandSQL("CREATE CLASS " + clazz + " EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'aaa'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'bbb'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'ccc'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + clazz + " SET name = 'ddd'")).execute();

    db.command(
            new OCommandSQL(
                "CREATE EDGE E FROM (SELECT FROM "
                    + clazz
                    + " WHERE name = 'aaa') TO (SELECT FROM "
                    + clazz
                    + " WHERE name = 'bbb')"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE E FROM (SELECT FROM "
                    + clazz
                    + " WHERE name = 'bbb') TO (SELECT FROM "
                    + clazz
                    + " WHERE name = 'ccc')"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE E FROM (SELECT FROM "
                    + clazz
                    + " WHERE name = 'ccc') TO (SELECT FROM "
                    + clazz
                    + " WHERE name = 'ddd')"))
        .execute();

    String query =
        "MATCH { class: "
            + clazz
            + ", as:a, where:(name = 'aaa')} --> {as:b, while:($depth<10), pathAlias: xy} RETURN a.name as name, b.name as bname, xy";

    OResultSet result = db.query(query);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Object path = item.getProperty("xy");
      Assert.assertTrue(path instanceof List);
      List<OIdentifiable> thePath = (List<OIdentifiable>) path;

      String bname = item.getProperty("bname");
      if (bname.equals("aaa")) {
        Assert.assertEquals(0, thePath.size());
      } else if (bname.equals("aaa")) {
        Assert.assertEquals(1, thePath.size());
        Assert.assertEquals("bbb", ((OElement) thePath.get(0).getRecord()).getProperty("name"));
      } else if (bname.equals("ccc")) {
        Assert.assertEquals(2, thePath.size());
        Assert.assertEquals("bbb", ((OElement) thePath.get(0).getRecord()).getProperty("name"));
        Assert.assertEquals("ccc", ((OElement) thePath.get(1).getRecord()).getProperty("name"));
      } else if (bname.equals("ddd")) {
        Assert.assertEquals(3, thePath.size());
        Assert.assertEquals("bbb", ((OElement) thePath.get(0).getRecord()).getProperty("name"));
        Assert.assertEquals("ccc", ((OElement) thePath.get(1).getRecord()).getProperty("name"));
        Assert.assertEquals("ddd", ((OElement) thePath.get(2).getRecord()).getProperty("name"));
      }
    }
    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testClusterTarget() {
    String clazz = "testClusterTarget";
    db.command("CREATE CLASS " + clazz + " EXTENDS V").close();
    db.command("ALTER CLASS " + clazz + " ADDCLUSTER " + clazz.toLowerCase() + "_one").close();
    db.command("ALTER CLASS " + clazz + " ADDCLUSTER " + clazz.toLowerCase() + "_two").close();
    db.command("ALTER CLASS " + clazz + " ADDCLUSTER " + clazz.toLowerCase() + "_three").close();

    OVertex v1 = db.newVertex(clazz);
    v1.setProperty("name", "one");
    v1.save(clazz.toLowerCase() + "_one");

    OVertex vx = db.newVertex(clazz);
    vx.setProperty("name", "onex");
    vx.save(clazz.toLowerCase() + "_one");

    OVertex v2 = db.newVertex(clazz);
    v2.setProperty("name", "two");
    v2.save(clazz.toLowerCase() + "_two");

    OVertex v3 = db.newVertex(clazz);
    v3.setProperty("name", "three");
    v3.save(clazz.toLowerCase() + "_three");

    v1.addEdge(v2).save();
    v2.addEdge(v3).save();
    v1.addEdge(v3).save();

    String query =
        "MATCH { cluster: "
            + clazz.toLowerCase()
            + "_one, as:a} --> {as:b, cluster:"
            + clazz.toLowerCase()
            + "_two} RETURN a.name as aname, b.name as bname";

    OResultSet result = db.query(query);

    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("one", item.getProperty("aname"));
    Assert.assertEquals("two", item.getProperty("bname"));

    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testNegativePattern() {
    String clazz = "testNegativePattern";
    db.command("CREATE CLASS " + clazz + " EXTENDS V").close();

    OVertex v1 = db.newVertex(clazz);
    v1.setProperty("name", "a");
    v1.save();

    OVertex v2 = db.newVertex(clazz);
    v2.setProperty("name", "b");
    v2.save();

    OVertex v3 = db.newVertex(clazz);
    v3.setProperty("name", "c");
    v3.save();

    v1.addEdge(v2).save();
    v2.addEdge(v3).save();

    String query = "MATCH { class:" + clazz + ", as:a} --> {as:b} --> {as:c}, ";
    query += " NOT {as:a} --> {as:c}";
    query += " RETURN $patterns";

    OResultSet result = db.query(query);
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testNegativePattern2() {
    String clazz = "testNegativePattern2";
    db.command("CREATE CLASS " + clazz + " EXTENDS V").close();

    OVertex v1 = db.newVertex(clazz);
    v1.setProperty("name", "a");
    v1.save();

    OVertex v2 = db.newVertex(clazz);
    v2.setProperty("name", "b");
    v2.save();

    OVertex v3 = db.newVertex(clazz);
    v3.setProperty("name", "c");
    v3.save();

    v1.addEdge(v2).save();
    v2.addEdge(v3).save();
    v1.addEdge(v3).save();

    String query = "MATCH { class:" + clazz + ", as:a} --> {as:b} --> {as:c}, ";
    query += " NOT {as:a} --> {as:c}";
    query += " RETURN $patterns";

    OResultSet result = db.query(query);
    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testNegativePattern3() {
    String clazz = "testNegativePattern3";
    db.command("CREATE CLASS " + clazz + " EXTENDS V").close();

    OVertex v1 = db.newVertex(clazz);
    v1.setProperty("name", "a");
    v1.save();

    OVertex v2 = db.newVertex(clazz);
    v2.setProperty("name", "b");
    v2.save();

    OVertex v3 = db.newVertex(clazz);
    v3.setProperty("name", "c");
    v3.save();

    v1.addEdge(v2).save();
    v2.addEdge(v3).save();
    v1.addEdge(v3).save();

    String query = "MATCH { class:" + clazz + ", as:a} --> {as:b} --> {as:c}, ";
    query += " NOT {as:a} --> {as:c, where:(name <> 'c')}";
    query += " RETURN $patterns";

    OResultSet result = db.query(query);
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testPathTraversal() {
    String clazz = "testPathTraversal";
    db.command("CREATE CLASS " + clazz + " EXTENDS V").close();

    OVertex v1 = db.newVertex(clazz);
    v1.setProperty("name", "a");
    v1.save();

    OVertex v2 = db.newVertex(clazz);
    v2.setProperty("name", "b");
    v2.save();

    OVertex v3 = db.newVertex(clazz);
    v3.setProperty("name", "c");
    v3.save();

    v1.setProperty("next", v2);
    v2.setProperty("next", v3);

    v1.save();
    v2.save();

    String query = "MATCH { class:" + clazz + ", as:a}.next{as:b, where:(name ='b')}";
    query += " RETURN a.name as a, b.name as b";

    OResultSet result = db.query(query);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals("a", item.getProperty("a"));
    Assert.assertEquals("b", item.getProperty("b"));

    Assert.assertFalse(result.hasNext());

    result.close();

    query = "MATCH { class:" + clazz + ", as:a, where:(name ='a')}.next{as:b}";
    query += " RETURN a.name as a, b.name as b";

    result = db.query(query);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    Assert.assertEquals("a", item.getProperty("a"));
    Assert.assertEquals("b", item.getProperty("b"));

    Assert.assertFalse(result.hasNext());

    result.close();
  }

  private OResultSet getManagedPathElements(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("  match {class:Employee, as:boss, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}<-ParentDepartment-{");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return distinct $pathElements");

    return db.query(query.toString());
  }

  @Test
  public void testQuotedClassName() {
    String className = "testQuotedClassName";
    db.command(new OCommandSQL("CREATE CLASS " + className + " EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE VERTEX " + className + " SET name = 'a'")).execute();

    String query = "MATCH {class: `" + className + "`, as:foo} RETURN $elements";

    try (OResultSet rs = db.query(query)) {
      Assert.assertEquals(1L, rs.stream().count());
    }
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

  private static OProfiler getProfilerInstance() throws Exception {
    return Orient.instance().getProfiler();
  }

  private void printExecutionPlan(OResultSet result) {
    printExecutionPlan(null, result);
  }

  private void printExecutionPlan(String query, OResultSet result) {
    //    if (query != null) {
    //      System.out.println(query);
    //    }
    //    result.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 3)));
    //    System.out.println();
  }
}
