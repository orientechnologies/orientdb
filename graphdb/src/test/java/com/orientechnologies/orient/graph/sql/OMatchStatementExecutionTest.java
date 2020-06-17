package com.orientechnologies.orient.graph.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OMatchStatementExecutionTest {
  private static String DB_STORAGE = "memory";
  private static String DB_NAME = "OMatchStatementExecutionTest";

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
      db.command(
              new OCommandSQL(
                  "CREATE EDGE IndexedEDGE FROM (SELECT FROM IndexedVertex WHERE uid = 0) TO (SELECT FROM IndexedVertex WHERE uid > "
                      + (i * nodes / 100)
                      + " and uid <"
                      + ((i + 1) * nodes / 100)
                      + ")"))
          .execute();
    }

    for (int i = 0; i < 100; i++) {
      db.command(
              new OCommandSQL(
                  "CREATE EDGE IndexedEDGE FROM (SELECT FROM IndexedVertex WHERE uid > "
                      + ((i * nodes / 100) + 1)
                      + " and uid < "
                      + (((i + 1) * nodes / 100) + 1)
                      + ") TO (SELECT FROM IndexedVertex WHERE uid = 1)"))
          .execute();
    }
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
    List<ODocument> qResult =
        db.command(new OCommandSQL("match {class:Person, as: person} return person")).execute();
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
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, as: person, where: (name = 'n1' or name = 'n2')} return person"))
            .execute();

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
  public void testSimpleLimit() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit 1"))
            .execute();

    assertEquals(1, qResult.size());
  }

  @Test
  public void testSimpleLimit2() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit -1"))
            .execute();

    assertEquals(2, qResult.size());
  }

  @Test
  public void testSimpleLimit3() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit 3"))
            .execute();

    assertEquals(2, qResult.size());
  }

  @Test
  public void testSimpleUnnamedParams() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, as: person, where: (name = ? or name = ?)} return person"))
            .execute("n1", "n2");

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

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return $matches)"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).field("name"));
  }

  @Test
  public void testCommonFriendsArrows() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return $matches)"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).field("name"));
  }

  @Test
  public void testCommonFriends2() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return friend.name as name"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).field("name"));
  }

  @Test
  public void testCommonFriends2Arrows() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return friend.name as name"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).field("name"));
  }

  @Test
  public void testReturnMethod() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return friend.name.toUpperCase(Locale.ENGLISH) as name"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("N2", qResult.get(0).field("name"));
  }

  @Test
  public void testReturnMethodArrows() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return friend.name.toUpperCase(Locale.ENGLISH) as name"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("N2", qResult.get(0).field("name"));
  }

  @Test
  public void testReturnExpression() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return friend.name + ' ' +friend.name as name"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2 n2", qResult.get(0).field("name"));
  }

  @Test
  public void testReturnExpressionArrows() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return friend.name + ' ' +friend.name as name"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2 n2", qResult.get(0).field("name"));
  }

  @Test
  public void testReturnDefaultAlias() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){class: Person, where:(name = 'n4')} return friend.name"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).getProperty("friend.name"));
  }

  @Test
  public void testReturnDefaultAliasArrows() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{class: Person, where:(name = 'n4')} return friend.name"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).getProperty("friend.name"));
  }

  @Test
  public void testFriendsOfFriends() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend').out('Friend'){as:friend} return $matches)"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n4", qResult.get(0).field("name"));
  }

  @Test
  public void testFriendsOfFriendsArrows() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{}-Friend->{as:friend} return $matches)"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n4", qResult.get(0).field("name"));
  }

  @Test
  public void testFriendsOfFriends2() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1'), as: me}.both('Friend').both('Friend'){as:friend, where: ($matched.me != $currentMatch)} return $matches)"))
            .execute();

    for (ODocument doc : qResult) {
      assertNotEquals(doc.field("name"), "n1");
    }
  }

  @Test
  public void testFriendsOfFriends2Arrows() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1'), as: me}-Friend-{}-Friend-{as:friend, where: ($matched.me != $currentMatch)} return $matches)"))
            .execute();

    for (ODocument doc : qResult) {
      assertNotEquals(doc.field("name"), "n1");
    }
  }

  @Test
  public void testFriendsWithName() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1' and 1 + 1 = 2)}.out('Friend'){as:friend, where:(name = 'n2' and 1 + 1 = 2)} return friend)"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).field("name"));
  }

  @Test
  public void testFriendsWithNameArrows() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1' and 1 + 1 = 2)}-Friend->{as:friend, where:(name = 'n2' and 1 + 1 = 2)} return friend)"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).field("name"));
  }

  @Test
  public void testWhile() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 1)} return friend)"))
            .execute();
    assertEquals(3, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 2), where: ($depth=1) } return friend)"))
            .execute();
    assertEquals(2, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 4), where: ($depth=1) } return friend)"))
            .execute();
    assertEquals(2, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend)"))
            .execute();
    assertEquals(6, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend limit 3)"))
            .execute();
    assertEquals(3, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend) limit 3"))
            .execute();
    assertEquals(3, qResult.size());
  }

  @Test
  public void testWhileArrows() throws Exception {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 1)} return friend)"))
            .execute();
    assertEquals(3, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 2), where: ($depth=1) } return friend)"))
            .execute();
    assertEquals(2, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 4), where: ($depth=1) } return friend)"))
            .execute();
    assertEquals(2, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, while: (true) } return friend)"))
            .execute();
    assertEquals(6, qResult.size());
  }

  @Test
  public void testMaxDepth() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth=1) } return friend)"))
            .execute();
    assertEquals(2, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1 } return friend)"))
            .execute();
    assertEquals(3, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 0 } return friend)"))
            .execute();
    assertEquals(1, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth > 0) } return friend)"))
            .execute();
    assertEquals(2, qResult.size());
  }

  @Test
  public void testMaxDepthArrow() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1, where: ($depth=1) } return friend)"))
            .execute();
    assertEquals(2, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1 } return friend)"))
            .execute();
    assertEquals(3, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 0 } return friend)"))
            .execute();
    assertEquals(1, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from (match {class:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1, where: ($depth > 0) } return friend)"))
            .execute();
    assertEquals(2, qResult.size());
  }

  @Test
  public void testManager() {
    // the manager of a person is the manager of the department that person belongs to.
    // if that department does not have a direct manager, climb up the hierarchy until you find one
    assertEquals("c", getManager("p10").field("name"));
    assertEquals("c", getManager("p12").field("name"));
    assertEquals("b", getManager("p6").field("name"));
    assertEquals("b", getManager("p11").field("name"));

    assertEquals("c", getManagerArrows("p10").field("name"));
    assertEquals("c", getManagerArrows("p12").field("name"));
    assertEquals("b", getManagerArrows("p6").field("name"));
    assertEquals("b", getManagerArrows("p11").field("name"));
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

    List<OIdentifiable> qResult = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, qResult.size());
    return qResult.get(0).getRecord();
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

    List<OIdentifiable> qResult = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, qResult.size());
    return qResult.get(0).getRecord();
  }

  @Test
  public void testManager2() {
    // the manager of a person is the manager of the department that person belongs to.
    // if that department does not have a direct manager, climb up the hierarchy until you find one
    assertEquals("c", getManager2("p10").field("name"));
    assertEquals("c", getManager2("p12").field("name"));
    assertEquals("b", getManager2("p6").field("name"));
    assertEquals("b", getManager2("p11").field("name"));

    assertEquals("c", getManager2Arrows("p10").field("name"));
    assertEquals("c", getManager2Arrows("p12").field("name"));
    assertEquals("b", getManager2Arrows("p6").field("name"));
    assertEquals("b", getManager2Arrows("p11").field("name"));
  }

  private ODocument getManager2(String personName) {
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

    List<OIdentifiable> qResult = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, qResult.size());
    return qResult.get(0).getRecord();
  }

  private ODocument getManager2Arrows(String personName) {
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

    List<OIdentifiable> qResult = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, qResult.size());
    return qResult.get(0).getRecord();
  }

  @Test
  public void testManaged() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    List<OIdentifiable> managedByA = getManagedBy("a");
    assertEquals(1, managedByA.size());
    assertEquals("p1", ((ODocument) managedByA.get(0).getRecord()).field("name"));

    List<OIdentifiable> managedByB = getManagedBy("b");
    assertEquals(5, managedByB.size());
    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (OIdentifiable id : managedByB) {
      ODocument doc = id.getRecord();
      String name = doc.field("name");
      names.add(name);
    }
    assertEquals(expectedNames, names);
  }

  private List<OIdentifiable> getManagedBy(String managerName) {
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

    return db.command(new OCommandSQL(query.toString())).execute();
  }

  @Test
  public void testManagedArrows() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    List<OIdentifiable> managedByA = getManagedByArrows("a");
    assertEquals(1, managedByA.size());
    assertEquals("p1", ((ODocument) managedByA.get(0).getRecord()).field("name"));

    List<OIdentifiable> managedByB = getManagedByArrows("b");
    assertEquals(5, managedByB.size());
    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (OIdentifiable id : managedByB) {
      ODocument doc = id.getRecord();
      String name = doc.field("name");
      names.add(name);
    }
    assertEquals(expectedNames, names);
  }

  private List<OIdentifiable> getManagedByArrows(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(managed) from (");
    query.append("  match {class:Employee, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}<-ParentDepartment-{");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return managed");
    query.append(")");

    return db.command(new OCommandSQL(query.toString())).execute();
  }

  @Test
  public void testManaged2() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    List<OIdentifiable> managedByA = getManagedBy2("a");
    assertEquals(1, managedByA.size());
    assertEquals("p1", ((ODocument) managedByA.get(0).getRecord()).field("name"));

    List<OIdentifiable> managedByB = getManagedBy2("b");
    assertEquals(5, managedByB.size());
    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (OIdentifiable id : managedByB) {
      ODocument doc = id.getRecord();
      String name = doc.field("name");
      names.add(name);
    }
    assertEquals(expectedNames, names);
  }

  private List<OIdentifiable> getManagedBy2(String managerName) {
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

    return db.command(new OCommandSQL(query.toString())).execute();
  }

  @Test
  public void testManaged2Arrows() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    List<OIdentifiable> managedByA = getManagedBy2Arrows("a");
    assertEquals(1, managedByA.size());
    assertEquals("p1", ((ODocument) managedByA.get(0).getRecord()).field("name"));

    List<OIdentifiable> managedByB = getManagedBy2Arrows("b");
    assertEquals(5, managedByB.size());
    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (OIdentifiable id : managedByB) {
      ODocument doc = id.getRecord();
      String name = doc.field("name");
      names.add(name);
    }
    assertEquals(expectedNames, names);
  }

  private List<OIdentifiable> getManagedBy2Arrows(String managerName) {
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

    return db.command(new OCommandSQL(query.toString())).execute();
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

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
  }

  @Test
  public void testTriangle1Arrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append(
        "{class:TriangleV, as: friend1, where: (uid = 0)} -TriangleE-> {as: friend2} -TriangleE-> {as: friend3},");
    query.append("{class:TriangleV, as: friend1} -TriangleE-> {as: friend3}");
    query.append("return $matches");

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
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

    List<ODocument> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    ODocument doc = result.get(0);
    ODocument friend1 = ((OIdentifiable) doc.field("friend1")).getRecord();
    ODocument friend2 = ((OIdentifiable) doc.field("friend2")).getRecord();
    ODocument friend3 = ((OIdentifiable) doc.field("friend3")).getRecord();
    assertEquals(0, friend1.<Object>field("uid"));
    assertEquals(1, friend2.<Object>field("uid"));
    assertEquals(2, friend3.<Object>field("uid"));
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

    List<ODocument> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    ODocument doc = result.get(0);
    ODocument friend1 = ((OIdentifiable) doc.field("friend1")).getRecord();
    ODocument friend2 = ((OIdentifiable) doc.field("friend2")).getRecord();
    ODocument friend3 = ((OIdentifiable) doc.field("friend3")).getRecord();
    assertEquals(0, friend1.<Object>field("uid"));
    assertEquals(1, friend2.<Object>field("uid"));
    assertEquals(2, friend3.<Object>field("uid"));
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

    List<ODocument> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    ODocument doc = result.get(0);
    ODocument friend1 = ((OIdentifiable) doc.field("friend1")).getRecord();
    ODocument friend2 = ((OIdentifiable) doc.field("friend2")).getRecord();
    ODocument friend3 = ((OIdentifiable) doc.field("friend3")).getRecord();
    assertEquals(0, friend1.<Object>field("uid"));
    assertEquals(1, friend2.<Object>field("uid"));
    assertEquals(2, friend3.<Object>field("uid"));
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

    List<ODocument> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
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

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
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

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
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

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
  }

  @Test
  public void testCartesianProduct() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where:(uid = 1)},");
    query.append("{class:TriangleV, as: friend2, where:(uid = 2 or uid = 3)}");
    query.append("return $matches");

    List<OIdentifiable> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(2, result.size());
    for (OIdentifiable d : result) {
      assertEquals(
          ((ODocument) ((ODocument) d.getRecord()).field("friend1")).<Object>field("uid"), 1);
    }
  }

  @Test
  public void testCartesianProductLimit() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where:(uid = 1)},");
    query.append("{class:TriangleV, as: friend2, where:(uid = 2 or uid = 3)}");
    query.append("return $matches LIMIT 1");

    List<OIdentifiable> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    for (OIdentifiable d : result) {
      assertEquals(
          ((ODocument) ((ODocument) d.getRecord()).field("friend1")).<Object>field("uid"), 1);
    }
  }

  @Test
  public void testArrayNumber() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0] as foo");

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    ODocument doc = (ODocument) result.get(0);
    Object foo = doc.field("foo");
    assertNotNull(foo);
    assertTrue(foo instanceof OVertex);
  }

  @Test
  public void testArraySingleSelectors2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0,1] as foo");

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    ODocument doc = (ODocument) result.get(0);
    Object foo = doc.field("foo");
    assertNotNull(foo);
    assertTrue(foo instanceof List);
    assertEquals(2, ((List) foo).size());
  }

  @Test
  public void testArrayRangeSelectors1() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..1] as foo");

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    ODocument doc = (ODocument) result.get(0);
    Object foo = doc.field("foo");
    assertNotNull(foo);
    assertTrue(foo instanceof List);
    assertEquals(1, ((List) foo).size());
  }

  @Test
  public void testArrayRange2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..2] as foo");

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    ODocument doc = (ODocument) result.get(0);
    Object foo = doc.field("foo");
    assertNotNull(foo);
    assertTrue(foo instanceof List);
    assertEquals(2, ((List) foo).size());
  }

  @Test
  public void testArrayRange3() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..3] as foo");

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    ODocument doc = (ODocument) result.get(0);
    Object foo = doc.field("foo");
    assertNotNull(foo);
    assertTrue(foo instanceof List);
    assertEquals(2, ((List) foo).size());
  }

  @Test
  public void testConditionInSquareBrackets() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[uid = 2] as foo");

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    ODocument doc = (ODocument) result.get(0);
    Object foo = doc.field("foo");
    assertNotNull(foo);
    assertTrue(foo instanceof List);
    assertEquals(1, ((List) foo).size());
    OVertex resultVertex = (OVertex) ((List) foo).get(0);
    assertEquals(2, resultVertex.<Object>getProperty("uid"));
  }

  @Test
  public void testIndexedEdge() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)}");
    query.append(".out('IndexedEdge'){class:IndexedVertex, as: two, where: (uid = 1)}");
    query.append("return one, two");

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
  }

  @Test
  public void testIndexedEdgeArrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)}");
    query.append("-IndexedEdge->{class:IndexedVertex, as: two, where: (uid = 1)}");
    query.append("return one, two");

    List<?> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
  }

  @Test
  public void testJson() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)} ");
    query.append("return {'name':'foo', 'uuid':one.uid}");

    List<ODocument> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    //    ODocument doc = result.get(0);
    //    assertEquals("foo", doc.field("name"));
    //    assertEquals(0, doc.field("uuid"));
  }

  @Test
  public void testJson2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)} ");
    query.append("return {'name':'foo', 'sub': {'uuid':one.uid}}");

    List<ODocument> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    //    ODocument doc = result.get(0);
    //    assertEquals("foo", doc.field("name"));
    //    assertEquals(0, doc.field("sub.uuid"));
  }

  @Test
  public void testJson3() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{class:IndexedVertex, as: one, where: (uid = 0)} ");
    query.append("return {'name':'foo', 'sub': [{'uuid':one.uid}]}");

    List<ODocument> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    //    ODocument doc = result.get(0);
    //    assertEquals("foo", doc.field("name"));
    //    assertEquals(0, doc.field("sub[0].uuid"));
  }

  @Test
  public void testUnique() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append(
        "{class:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return one, two");

    List<ODocument> result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());

    query = new StringBuilder();
    query.append("match ");
    query.append(
        "{class:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return one.uid, two.uid");

    result = db.command(new OCommandSQL(query.toString())).execute();
    assertEquals(1, result.size());
    //    ODocument doc = result.get(0);
    //    assertEquals("foo", doc.field("name"));
    //    assertEquals(0, doc.field("sub[0].uuid"));
  }

  @Test
  public void testManagedElements() {
    List<OIdentifiable> managedByB = getManagedElements("b");
    assertEquals(6, managedByB.size());
    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("b");
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (OIdentifiable id : managedByB) {
      ODocument doc = id.getRecord();
      String name = doc.field("name");
      names.add(name);
    }
    assertEquals(expectedNames, names);
  }

  private List<OIdentifiable> getManagedElements(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("  match {class:Employee, as:boss, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}<-ParentDepartment-{");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return $elements");

    return db.command(new OCommandSQL(query.toString())).execute();
  }

  @Test
  public void testManagedPathElements() {
    List<OIdentifiable> managedByB = getManagedPathElements("b");
    assertEquals(10, managedByB.size());
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
    for (OIdentifiable id : managedByB) {
      ODocument doc = id.getRecord();
      String name = doc.field("name");
      names.add(name);
    }
    assertEquals(expectedNames, names);
  }

  @Test
  public void testOptional() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, as: person} -NonExistingEdge-> {as:b, optional:true} return person, b.name"))
            .execute();
    assertEquals(6, qResult.size());
    for (ODocument doc : qResult) {
      assertTrue(doc.fieldNames().length == 2);
      OIdentifiable personId = doc.field("person");
      ODocument person = personId.getRecord();
      String name = person.field("name");
      assertTrue(name.startsWith("n"));
    }
  }

  @Test
  public void testOptional2() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "match {class:Person, as: person} --> {as:b, optional:true, where:(nonExisting = 12)} return person, b.name"))
            .execute();
    assertEquals(6, qResult.size());
    for (ODocument doc : qResult) {
      assertTrue(doc.fieldNames().length == 2);
      OIdentifiable personId = doc.field("person");
      ODocument person = personId.getRecord();
      String name = person.field("name");
      assertTrue(name.startsWith("n"));
    }
  }

  @Test
  public void testOptional3() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select friend.name as name from ("
                        + "match {class:Person, as:a, where:(name = 'n1' and 1 + 1 = 2)}.out('Friend'){as:friend, where:(name = 'n2' and 1 + 1 = 2)},"
                        + "{as:a}.out(){as:b, where:(nonExisting = 12), optional:true},"
                        + "{as:friend}.out(){as:b, optional:true}"
                        + " return friend)"))
            .execute();
    assertEquals(1, qResult.size());
    assertEquals("n2", qResult.get(0).field("name"));
  }

  @Test
  public void testAliasesWithSubquery() throws Exception {
    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select from ( match {class:Person, as:A} return A.name as namexx ) limit 1"))
            .execute();
    assertEquals(1, qResult.size());
    assertNotNull(qResult.get(0).field("namexx"));
    assertTrue(qResult.get(0).field("namexx").toString().startsWith("n"));
  }

  @Test
  public void testEvalInReturn() {
    // issue #6606
    db.command(new OCommandSQL("CREATE CLASS testEvalInReturn EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY testEvalInReturn.name String")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testEvalInReturn SET name = 'foo'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testEvalInReturn SET name = 'bar'")).execute();

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: testEvalInReturn, as: p} RETURN if(eval(\"p.name = 'foo'\"), 1, 2) AS b"))
            .execute();

    assertEquals(2, qResult.size());
    int sum = 0;
    for (ODocument doc : qResult) {
      sum += ((Number) doc.field("b")).intValue();
    }
    assertEquals(3, sum);

    // check that it still removes duplicates (standard behavior for MATCH)
    qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: testEvalInReturn, as: p} RETURN if(eval(\"p.name = 'foo'\"), 'foo', 'foo') AS b"))
            .execute();

    assertEquals(1, qResult.size());
  }

  @Test
  public void testCheckClassAsCondition() {

    db.command(new OCommandSQL("CREATE CLASS testCheckClassAsCondition EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testCheckClassAsCondition1 EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testCheckClassAsCondition2 EXTENDS V")).execute();

    db.command(new OCommandSQL("CREATE VERTEX testCheckClassAsCondition SET name = 'foo'"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX testCheckClassAsCondition1 SET name = 'bar'"))
        .execute();
    for (int i = 0; i < 5; i++) {
      db.command(new OCommandSQL("CREATE VERTEX testCheckClassAsCondition2 SET name = 'baz'"))
          .execute();
    }
    db.command(
            new OCommandSQL(
                "CREATE EDGE E FROM (select from testCheckClassAsCondition where name = 'foo') to (select from testCheckClassAsCondition1)"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE E FROM (select from testCheckClassAsCondition where name = 'foo') to (select from testCheckClassAsCondition2)"))
        .execute();

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: testCheckClassAsCondition, as: p} -E- {class: testCheckClassAsCondition1, as: q} RETURN $elements"))
            .execute();

    assertEquals(2, qResult.size());
  }

  @Test
  public void testInstanceof() {

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: Person, as: p, where: ($currentMatch instanceof 'Person')} return $elements limit 1"))
            .execute();
    assertEquals(1, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: Person, as: p, where: ($currentMatch instanceof 'V')} return $elements limit 1"))
            .execute();
    assertEquals(1, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: Person, as: p, where: (not ($currentMatch instanceof 'Person'))} return $elements limit 1"))
            .execute();
    assertEquals(0, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: Person, where: (name = 'n1')}.out(){as:p, where: ($currentMatch instanceof 'Person')} return $elements limit 1"))
            .execute();
    assertEquals(1, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: Person, where: (name = 'n1')}.out(){as:p, where: ($currentMatch instanceof 'Person' and '$currentMatch' <> '@this')} return $elements limit 1"))
            .execute();
    assertEquals(1, qResult.size());

    qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: Person, where: (name = 'n1')}.out(){as:p, where: ( not ($currentMatch instanceof 'Person'))} return $elements limit 1"))
            .execute();
    assertEquals(0, qResult.size());
  }

  @Test
  public void testBigEntryPoint() {
    // issue #6890

    OSchema schema = db.getMetadata().getSchema();
    schema.createClass("testBigEntryPoint1");
    schema.createClass("testBigEntryPoint2");

    for (int i = 0; i < 1000; i++) {
      ODocument doc = db.newInstance("testBigEntryPoint1");
      doc.field("a", i);
      doc.save();
    }

    ODocument doc = db.newInstance("testBigEntryPoint2");
    doc.field("b", "b");
    doc.save();

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "MATCH {class: testBigEntryPoint1, as: a}, {class: testBigEntryPoint2, as: b} return $elements limit 1"))
            .execute();
    assertEquals(1, qResult.size());
  }

  @Test
  public void testMatched1() {
    // issue #6931
    db.command(new OCommandSQL("CREATE CLASS testMatched1_Foo EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testMatched1_Bar EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testMatched1_Baz EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testMatched1_Far EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testMatched1_Foo_Bar EXTENDS E")).execute();
    db.command(new OCommandSQL("CREATE CLASS testMatched1_Bar_Baz EXTENDS E")).execute();
    db.command(new OCommandSQL("CREATE CLASS testMatched1_Foo_Far EXTENDS E")).execute();

    db.command(new OCommandSQL("CREATE VERTEX testMatched1_Foo SET name = 'foo'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testMatched1_Bar SET name = 'bar'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testMatched1_Baz SET name = 'baz'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testMatched1_Far SET name = 'far'")).execute();

    db.command(
            new OCommandSQL(
                "CREATE EDGE testMatched1_Foo_Bar FROM (SELECT FROM testMatched1_Foo) TO (SELECT FROM testMatched1_Bar)"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE testMatched1_Bar_Baz FROM (SELECT FROM testMatched1_Bar) TO (SELECT FROM testMatched1_Baz)"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE testMatched1_Foo_Far FROM (SELECT FROM testMatched1_Foo) TO (SELECT FROM testMatched1_Far)"))
        .execute();

    List result =
        db.query(
            new OSQLSynchQuery(
                "MATCH \n"
                    + "{class: testMatched1_Foo, as: foo}.out('testMatched1_Foo_Bar') {as: bar}, \n"
                    + "{class: testMatched1_Bar,as: bar}.out('testMatched1_Bar_Baz') {as: baz}, \n"
                    + "{class: testMatched1_Foo,as: foo}.out('testMatched1_Foo_Far') {where: ($matched.baz IS null),as: far}\n"
                    + "RETURN $matches"));
    assertTrue(result.isEmpty());

    result =
        db.query(
            new OSQLSynchQuery(
                "MATCH \n"
                    + "{class: testMatched1_Foo, as: foo}.out('testMatched1_Foo_Bar') {as: bar}, \n"
                    + "{class: testMatched1_Bar,as: bar}.out('testMatched1_Bar_Baz') {as: baz}, \n"
                    + "{class: testMatched1_Foo,as: foo}.out('testMatched1_Foo_Far') {where: ($matched.baz IS not null),as: far}\n"
                    + "RETURN $matches"));
    assertEquals(1, result.size());
  }

  @Test
  public void testDependencyOrdering1() {
    // issue #6931
    db.command(new OCommandSQL("CREATE CLASS testDependencyOrdering1_Foo EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testDependencyOrdering1_Bar EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testDependencyOrdering1_Baz EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testDependencyOrdering1_Far EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testDependencyOrdering1_Foo_Bar EXTENDS E")).execute();
    db.command(new OCommandSQL("CREATE CLASS testDependencyOrdering1_Bar_Baz EXTENDS E")).execute();
    db.command(new OCommandSQL("CREATE CLASS testDependencyOrdering1_Foo_Far EXTENDS E")).execute();

    db.command(new OCommandSQL("CREATE VERTEX testDependencyOrdering1_Foo SET name = 'foo'"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX testDependencyOrdering1_Bar SET name = 'bar'"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX testDependencyOrdering1_Baz SET name = 'baz'"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX testDependencyOrdering1_Far SET name = 'far'"))
        .execute();

    db.command(
            new OCommandSQL(
                "CREATE EDGE testDependencyOrdering1_Foo_Bar FROM ("
                    + "SELECT FROM testDependencyOrdering1_Foo) TO (SELECT FROM testDependencyOrdering1_Bar)"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE testDependencyOrdering1_Bar_Baz FROM ("
                    + "SELECT FROM testDependencyOrdering1_Bar) TO (SELECT FROM testDependencyOrdering1_Baz)"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE testDependencyOrdering1_Foo_Far FROM ("
                    + "SELECT FROM testDependencyOrdering1_Foo) TO (SELECT FROM testDependencyOrdering1_Far)"))
        .execute();

    // The correct but non-obvious execution order here is:
    // foo, bar, far, baz
    // This is a test to ensure that the query scheduler resolves dependencies correctly,
    // even if they are unusual or contrived.
    List result =
        db.query(
            new OSQLSynchQuery(
                "MATCH {\n"
                    + "    class: testDependencyOrdering1_Foo,\n"
                    + "    as: foo\n"
                    + "}.out('testDependencyOrdering1_Foo_Far') {\n"
                    + "    optional: true,\n"
                    + "    where: ($matched.bar IS NOT null),\n"
                    + "    as: far\n"
                    + "}, {\n"
                    + "    as: foo\n"
                    + "}.out('testDependencyOrdering1_Foo_Bar') {\n"
                    + "    where: ($matched.foo IS NOT null),\n"
                    + "    as: bar\n"
                    + "}.out('testDependencyOrdering1_Bar_Baz') {\n"
                    + "    where: ($matched.far IS NOT null),\n"
                    + "    as: baz\n"
                    + "} RETURN $matches"));
    assertEquals(1, result.size());
  }

  @Test
  public void testCircularDependency() {
    // issue #6931
    db.command(new OCommandSQL("CREATE CLASS testCircularDependency_Foo EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testCircularDependency_Bar EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testCircularDependency_Baz EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testCircularDependency_Far EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testCircularDependency_Foo_Bar EXTENDS E")).execute();
    db.command(new OCommandSQL("CREATE CLASS testCircularDependency_Bar_Baz EXTENDS E")).execute();
    db.command(new OCommandSQL("CREATE CLASS testCircularDependency_Foo_Far EXTENDS E")).execute();

    db.command(new OCommandSQL("CREATE VERTEX testCircularDependency_Foo SET name = 'foo'"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX testCircularDependency_Bar SET name = 'bar'"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX testCircularDependency_Baz SET name = 'baz'"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX testCircularDependency_Far SET name = 'far'"))
        .execute();

    db.command(
            new OCommandSQL(
                "CREATE EDGE testCircularDependency_Foo_Bar FROM ("
                    + "SELECT FROM testCircularDependency_Foo) TO (SELECT FROM testCircularDependency_Bar)"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE testCircularDependency_Bar_Baz FROM ("
                    + "SELECT FROM testCircularDependency_Bar) TO (SELECT FROM testCircularDependency_Baz)"))
        .execute();
    db.command(
            new OCommandSQL(
                "CREATE EDGE testCircularDependency_Foo_Far FROM ("
                    + "SELECT FROM testCircularDependency_Foo) TO (SELECT FROM testCircularDependency_Far)"))
        .execute();

    // The circular dependency here is:
    // - far depends on baz
    // - baz depends on bar
    // - bar depends on far
    OSQLSynchQuery query =
        new OSQLSynchQuery(
            "MATCH {\n"
                + "    class: testCircularDependency_Foo,\n"
                + "    as: foo\n"
                + "}.out('testCircularDependency_Foo_Far') {\n"
                + "    where: ($matched.baz IS NOT null),\n"
                + "    as: far\n"
                + "}, {\n"
                + "    as: foo\n"
                + "}.out('testCircularDependency_Foo_Bar') {\n"
                + "    where: ($matched.far IS NOT null),\n"
                + "    as: bar\n"
                + "}.out('testCircularDependency_Bar_Baz') {\n"
                + "    where: ($matched.bar IS NOT null),\n"
                + "    as: baz\n"
                + "} RETURN $matches");

    try {
      db.query(query);
      fail();
    } catch (OCommandExecutionException x) {
      // passed the test
    }
  }

  @Test
  public void testUndefinedAliasDependency() {
    // issue #6931
    db.command(new OCommandSQL("CREATE CLASS testUndefinedAliasDependency_Foo EXTENDS V"))
        .execute();
    db.command(new OCommandSQL("CREATE CLASS testUndefinedAliasDependency_Bar EXTENDS V"))
        .execute();
    db.command(new OCommandSQL("CREATE CLASS testUndefinedAliasDependency_Foo_Bar EXTENDS E"))
        .execute();

    db.command(new OCommandSQL("CREATE VERTEX testUndefinedAliasDependency_Foo SET name = 'foo'"))
        .execute();
    db.command(new OCommandSQL("CREATE VERTEX testUndefinedAliasDependency_Bar SET name = 'bar'"))
        .execute();

    db.command(
            new OCommandSQL(
                "CREATE EDGE testUndefinedAliasDependency_Foo_Bar FROM ("
                    + "SELECT FROM testUndefinedAliasDependency_Foo) TO (SELECT FROM testUndefinedAliasDependency_Bar)"))
        .execute();

    // "bar" in the following query declares a dependency on the alias "baz", which doesn't exist.
    OSQLSynchQuery query =
        new OSQLSynchQuery(
            "MATCH {\n"
                + "    class: testUndefinedAliasDependency_Foo,\n"
                + "    as: foo\n"
                + "}.out('testUndefinedAliasDependency_Foo_Bar') {\n"
                + "    where: ($matched.baz IS NOT null),\n"
                + "    as: bar\n"
                + "} RETURN $matches");

    try {
      db.query(query);
      fail();
    } catch (OCommandExecutionException x) {
      // passed the test
    }
  }

  @Test
  public void testCyclicDeepTraversal() {
    db.command(new OCommandSQL("CREATE CLASS testCyclicDeepTraversalV EXTENDS V")).execute();
    db.command(new OCommandSQL("CREATE CLASS testCyclicDeepTraversalE EXTENDS E")).execute();

    db.command(new OCommandSQL("CREATE VERTEX testCyclicDeepTraversalV SET name = 'a'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testCyclicDeepTraversalV SET name = 'b'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testCyclicDeepTraversalV SET name = 'c'")).execute();
    db.command(new OCommandSQL("CREATE VERTEX testCyclicDeepTraversalV SET name = 'z'")).execute();

    // a -> b -> z
    // z -> c -> a
    db.command(
            new OCommandSQL(
                "CREATE EDGE testCyclicDeepTraversalE from"
                    + "(select from testCyclicDeepTraversalV where name = 'a') to (select from testCyclicDeepTraversalV where name = 'b')"))
        .execute();

    db.command(
            new OCommandSQL(
                "CREATE EDGE testCyclicDeepTraversalE from"
                    + "(select from testCyclicDeepTraversalV where name = 'b') to (select from testCyclicDeepTraversalV where name = 'z')"))
        .execute();

    db.command(
            new OCommandSQL(
                "CREATE EDGE testCyclicDeepTraversalE from"
                    + "(select from testCyclicDeepTraversalV where name = 'z') to (select from testCyclicDeepTraversalV where name = 'c')"))
        .execute();

    db.command(
            new OCommandSQL(
                "CREATE EDGE testCyclicDeepTraversalE from"
                    + "(select from testCyclicDeepTraversalV where name = 'c') to (select from testCyclicDeepTraversalV where name = 'a')"))
        .execute();

    OSQLSynchQuery query =
        new OSQLSynchQuery(
            "MATCH {\n"
                + "    class: testCyclicDeepTraversalV,\n"
                + "    as: foo,\n"
                + "    where: (name = 'a')\n"
                + "}.out() {\n"
                + "    while: ($depth < 2),\n"
                + "    where: (name = 'z'),\n"
                + "    as: bar\n"
                + "}, {\n"
                + "    as: bar\n"
                + "}.out() {\n"
                + "    while: ($depth < 2),\n"
                + "    as: foo\n"
                + "} RETURN $patterns");

    List<?> result = db.query(query);
    assertEquals(1, result.size());
  }

  private List<OIdentifiable> getManagedPathElements(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("  match {class:Employee, as:boss, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}<-ParentDepartment-{");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return $pathElements");

    return db.command(new OCommandSQL(query.toString())).execute();
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
}
