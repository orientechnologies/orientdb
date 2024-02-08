package com.orientechnologies.orient.core.sql.executor.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OrientSql;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import com.orientechnologies.orient.core.sql.parser.SimpleNode;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OStatementIndexFinderTest {
  private ODatabaseSession session;
  private OrientDB orientDb;

  @Before
  public void before() {
    this.orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    this.orientDb.execute(
        "create database "
            + OStatementIndexFinderTest.class.getSimpleName()
            + " memory users (admin identified by 'adminpwd' role admin)");
    this.session =
        this.orientDb.open(OStatementIndexFinderTest.class.getSimpleName(), "admin", "adminpwd");
  }

  @Test
  public void simpleMatchTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    OSelectStatement stat = parseQuery("select from cl where name='a'");
    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void simpleRangeTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    OSelectStatement stat = parseQuery("select from cl where name > 'a'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Gt, result.get().getOperation());

    OSelectStatement stat1 = parseQuery("select from cl where name < 'a'");
    Optional<OIndexCandidate> result1 = stat1.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result1.get().getName());
    assertEquals(Operation.Lt, result1.get().getOperation());
  }

  @Test
  public void multipleSimpleAndMatchTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    OSelectStatement stat = parseQuery("select from cl where name='a' and name='b'");
    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof OMultipleIndexCanditate));
    OMultipleIndexCanditate multiple = (OMultipleIndexCanditate) result.get();
    assertEquals("cl.name", multiple.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, multiple.getCanditates().get(0).getOperation());
    assertEquals("cl.name", multiple.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, multiple.getCanditates().get(0).getOperation());
  }

  @Test
  public void requiredRangeOrMatchTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    OSelectStatement stat = parseQuery("select from cl where name='a' or name='b'");
    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    assertEquals("cl.name", required.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(0).getOperation());
    assertEquals("cl.name", required.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(1).getOperation());
  }

  @Test
  public void multipleRangeAndTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    OSelectStatement stat = parseQuery("select from cl where name < 'a' and name > 'b'");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof OMultipleIndexCanditate));
    OMultipleIndexCanditate multiple = (OMultipleIndexCanditate) result.get();
    assertEquals("cl.name", multiple.getCanditates().get(0).getName());
    assertEquals(Operation.Lt, multiple.getCanditates().get(0).getOperation());
    assertEquals("cl.name", multiple.getCanditates().get(1).getName());
    assertEquals(Operation.Gt, multiple.getCanditates().get(1).getOperation());
  }

  @Test
  public void requiredRangeOrTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    OSelectStatement stat = parseQuery("select from cl where name < 'a' or name > 'b'");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    assertEquals("cl.name", required.getCanditates().get(0).getName());
    assertEquals(Operation.Lt, required.getCanditates().get(0).getOperation());
    assertEquals("cl.name", required.getCanditates().get(1).getName());
    assertEquals(Operation.Gt, required.getCanditates().get(1).getOperation());
  }

  @Test
  public void simpleRangeNotTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    OSelectStatement stat = parseQuery("select from cl where not name < 'a' ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Ge, result.get().getOperation());
  }

  @Test
  public void simpleChainTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);
    OProperty prop1 = cl.createProperty("friend", OType.LINK, cl);
    prop1.createIndex(INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    OSelectStatement stat = parseQuery("select from cl where friend.friend.name = 'a' ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void simpleNestedAndOrMatchTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);
    OProperty prop1 = cl.createProperty("friend", OType.LINK, cl);
    prop1.createIndex(INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    OSelectStatement stat =
        parseQuery(
            "select from cl where (friend.name = 'a' and name='a') or (friend.name='b' and"
                + " name='b') ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);

    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    assertTrue((required.getCanditates().get(0) instanceof OMultipleIndexCanditate));
    OMultipleIndexCanditate first = (OMultipleIndexCanditate) required.getCanditates().get(0);
    assertEquals("cl.friend->cl.name->", first.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, first.getCanditates().get(0).getOperation());
    assertEquals("cl.name", first.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, first.getCanditates().get(1).getOperation());

    OMultipleIndexCanditate second = (OMultipleIndexCanditate) required.getCanditates().get(1);
    assertEquals("cl.friend->cl.name->", second.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, second.getCanditates().get(0).getOperation());
    assertEquals("cl.name", second.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, second.getCanditates().get(1).getOperation());
  }

  @Test
  public void simpleNestedAndOrPartialMatchTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    OSelectStatement stat =
        parseQuery(
            "select from cl where (friend.name = 'a' and name='a') or (friend.name='b' and"
                + " name='b') ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);

    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    OIndexCandidate first = required.getCanditates().get(0);
    assertEquals("cl.name", first.getName());
    assertEquals(Operation.Eq, first.getOperation());

    OIndexCandidate second = required.getCanditates().get(1);
    assertEquals("cl.name", second.getName());
    assertEquals(Operation.Eq, second.getOperation());
  }

  @Test
  public void simpleNestedOrNotMatchTest() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty("name", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);
    OProperty prop1 = cl.createProperty("friend", OType.LINK, cl);
    prop1.createIndex(INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    OSelectStatement stat =
        parseQuery(
            "select from cl where (friend.name = 'a' and name='a') or (friend.other='b' and"
                + " other='b') ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);

    assertFalse(result.isPresent());
  }

  @Test
  public void multivalueMatchTest() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty("name", OType.STRING);
    cl.createProperty("surname", OType.STRING);
    cl.createIndex("cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat = parseQuery("select from cl where name = 'a' and surname = 'b'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertEquals("cl.name_surname", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void multivalueMatchOneTest() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty("name", OType.STRING);
    cl.createProperty("surname", OType.STRING);
    cl.createIndex("cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat = parseQuery("select from cl where name = 'a' and other = 'b'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertEquals("cl.name_surname", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void multivalueNotMatchSecondPropertyTest() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty("name", OType.STRING);
    cl.createProperty("surname", OType.STRING);
    cl.createProperty("other", OType.STRING);
    cl.createIndex("cl.name_surname_other", INDEX_TYPE.NOTUNIQUE, "name", "surname", "other");

    OSelectStatement stat = parseQuery("select from cl where surname = 'a' and other = 'b'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertFalse(result.isPresent());
  }

  @Test
  public void multivalueNotMatchSecondPropertySingleConditionTest() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty("name", OType.STRING);
    cl.createProperty("surname", OType.STRING);
    cl.createIndex("cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat = parseQuery("select from cl where surname = 'a'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertFalse(result.isPresent());
  }

  @Test
  public void multivalueMatchPropertyORTest() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty("name", OType.STRING);
    cl.createProperty("surname", OType.STRING);
    cl.createIndex("cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat =
        parseQuery(
            "select from cl where (name = 'a' and surname = 'b') or (name='d' and surname='e')");

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertTrue(result.isPresent());
    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    assertEquals("cl.name_surname", required.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(0).getOperation());
    assertEquals("cl.name_surname", required.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(1).getOperation());
    assertEquals(required.getCanditates().size(), 2);
  }

  @Test
  public void multivalueNotMatchPropertyORTest() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty("name", OType.STRING);
    cl.createProperty("surname", OType.STRING);
    cl.createIndex("cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat =
        parseQuery(
            "select from cl where (name = 'a' and surname = 'b') or (other='d' and surname='e')");

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertFalse(result.isPresent());
  }

  @Test
  public void testMutipleConditionBetween() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty("name", OType.STRING);
    cl.createIndex("cl.name", INDEX_TYPE.NOTUNIQUE, "name");

    OSelectStatement stat = parseQuery("select from cl where name < 'a' and name > 'b'");
    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertTrue((result.get() instanceof ORangeIndexCanditate));
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Range, result.get().getOperation());
  }

  private OSelectStatement parseQuery(String query) {
    InputStream is = new ByteArrayInputStream(query.getBytes());
    OrientSql osql = new OrientSql(is);
    try {
      SimpleNode n = osql.parse();
      return (OSelectStatement) n;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void after() {
    this.session.close();
    this.orientDb.close();
  }
}
