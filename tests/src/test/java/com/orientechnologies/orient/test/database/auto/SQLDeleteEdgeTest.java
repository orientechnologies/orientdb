package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

import java.util.List;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 04/12/14
 */
public class SQLDeleteEdgeTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public SQLDeleteEdgeTest(@Optional String url) {
    super(url);
  }

  public void testDeleteFromTo() {
    database.command(new OCommandSQL("CREATE CLASS testFromToOneE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testFromToTwoE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testFromToV extends V")).execute();

    database.command(new OCommandSQL("create vertex testFromToV set name = 'Luca'")).execute();
    database.command(new OCommandSQL("create vertex testFromToV set name = 'Luca'")).execute();

    List<OIdentifiable> result = database.query(new OSQLSynchQuery<ODocument>("select from testFromToV"));

    database.command(
        new OCommandSQL("CREATE EDGE testFromToOneE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity()))
        .execute();
    database.command(
        new OCommandSQL("CREATE EDGE testFromToTwoE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity()))
        .execute();

    List<OIdentifiable> resultTwo = database.query(new OSQLSynchQuery<ODocument>("select expand(outE()) from "
        + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 2);

    database.command(
        new OCommandSQL("DELETE EDGE testFromToTwoE from " + result.get(1).getIdentity() + " to" + result.get(0).getIdentity()))
        .execute();

    resultTwo = database.query(new OSQLSynchQuery<ODocument>("select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 1);

    database.command(new OCommandSQL("DELETE FROM testFromToOneE")).execute();
    database.command(new OCommandSQL("DELETE FROM testFromToTwoE")).execute();
    database.command(new OCommandSQL("DELETE FROM testFromToV")).execute();
  }

  public void testDeleteFrom() {
    database.command(new OCommandSQL("CREATE CLASS testFromOneE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testFromTwoE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testFromV extends V")).execute();

    database.command(new OCommandSQL("create vertex testFromV set name = 'Luca'")).execute();
    database.command(new OCommandSQL("create vertex testFromV set name = 'Luca'")).execute();

    List<OIdentifiable> result = database.query(new OSQLSynchQuery<ODocument>("select from testFromV"));

    database.command(
        new OCommandSQL("CREATE EDGE testFromOneE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity()))
        .execute();
    database.command(
        new OCommandSQL("CREATE EDGE testFromTwoE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity()))
        .execute();

    List<OIdentifiable> resultTwo = database.query(new OSQLSynchQuery<ODocument>("select expand(outE()) from "
        + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 2);

    database.command(new OCommandSQL("DELETE EDGE testFromTwoE from " + result.get(1).getIdentity())).execute();

    resultTwo = database.query(new OSQLSynchQuery<ODocument>("select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 1);

    database.command(new OCommandSQL("DELETE FROM testFromOneE")).execute();
    database.command(new OCommandSQL("DELETE FROM testFromTwoE")).execute();
    database.command(new OCommandSQL("DELETE FROM testFromV")).execute();
  }

  public void testDeleteTo() {
    database.command(new OCommandSQL("CREATE CLASS testToOneE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testToTwoE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testToV extends V")).execute();

    database.command(new OCommandSQL("create vertex testToV set name = 'Luca'")).execute();
    database.command(new OCommandSQL("create vertex testToV set name = 'Luca'")).execute();

    List<OIdentifiable> result = database.query(new OSQLSynchQuery<ODocument>("select from testToV"));

    database.command(
        new OCommandSQL("CREATE EDGE testToOneE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity()))
        .execute();
    database.command(
        new OCommandSQL("CREATE EDGE testToTwoE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity()))
        .execute();

    List<OIdentifiable> resultTwo = database.query(new OSQLSynchQuery<ODocument>("select expand(outE()) from "
        + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 2);

    database.command(new OCommandSQL("DELETE EDGE testToTwoE to " + result.get(0).getIdentity())).execute();

    resultTwo = database.query(new OSQLSynchQuery<ODocument>("select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 1);

    database.command(new OCommandSQL("DELETE FROM testToOneE")).execute();
    database.command(new OCommandSQL("DELETE FROM testToTwoE")).execute();
    database.command(new OCommandSQL("DELETE FROM testToV")).execute();
  }
}