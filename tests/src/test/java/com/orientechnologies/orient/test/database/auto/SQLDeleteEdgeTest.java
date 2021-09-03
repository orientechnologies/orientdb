package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a
 *     href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
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

    List<OIdentifiable> result =
        database.query(new OSQLSynchQuery<ODocument>("select from testFromToV"));

    database
        .command(
            new OCommandSQL(
                "CREATE EDGE testFromToOneE from "
                    + result.get(1).getIdentity()
                    + " to "
                    + result.get(0).getIdentity()))
        .execute();
    database
        .command(
            new OCommandSQL(
                "CREATE EDGE testFromToTwoE from "
                    + result.get(1).getIdentity()
                    + " to "
                    + result.get(0).getIdentity()))
        .execute();

    List<OIdentifiable> resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 2);

    database
        .command(
            new OCommandSQL(
                "DELETE EDGE testFromToTwoE from "
                    + result.get(1).getIdentity()
                    + " to"
                    + result.get(0).getIdentity()))
        .execute();

    resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 1);

    database.command(new OCommandSQL("DELETE FROM testFromToOneE unsafe")).execute();
    database.command(new OCommandSQL("DELETE FROM testFromToTwoE unsafe")).execute();
    int deleted = database.command(new OCommandSQL("DELETE VERTEX testFromToV")).execute();
  }

  public void testDeleteFrom() {
    database.command(new OCommandSQL("CREATE CLASS testFromOneE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testFromTwoE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testFromV extends V")).execute();

    database.command(new OCommandSQL("create vertex testFromV set name = 'Luca'")).execute();
    database.command(new OCommandSQL("create vertex testFromV set name = 'Luca'")).execute();

    List<OIdentifiable> result =
        database.query(new OSQLSynchQuery<ODocument>("select from testFromV"));

    database
        .command(
            new OCommandSQL(
                "CREATE EDGE testFromOneE from "
                    + result.get(1).getIdentity()
                    + " to "
                    + result.get(0).getIdentity()))
        .execute();
    database
        .command(
            new OCommandSQL(
                "CREATE EDGE testFromTwoE from "
                    + result.get(1).getIdentity()
                    + " to "
                    + result.get(0).getIdentity()))
        .execute();

    List<OIdentifiable> resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 2);

    database
        .command(new OCommandSQL("DELETE EDGE testFromTwoE from " + result.get(1).getIdentity()))
        .execute();

    resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 1);

    database.command(new OCommandSQL("DELETE FROM testFromOneE unsafe")).execute();
    database.command(new OCommandSQL("DELETE FROM testFromTwoE unsafe")).execute();
    int deleted = database.command(new OCommandSQL("DELETE VERTEX testFromV")).execute();
  }

  public void testDeleteTo() {
    database.command(new OCommandSQL("CREATE CLASS testToOneE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testToTwoE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS testToV extends V")).execute();

    database.command(new OCommandSQL("create vertex testToV set name = 'Luca'")).execute();
    database.command(new OCommandSQL("create vertex testToV set name = 'Luca'")).execute();

    List<OIdentifiable> result =
        database.query(new OSQLSynchQuery<ODocument>("select from testToV"));

    database
        .command(
            new OCommandSQL(
                "CREATE EDGE testToOneE from "
                    + result.get(1).getIdentity()
                    + " to "
                    + result.get(0).getIdentity()))
        .execute();
    database
        .command(
            new OCommandSQL(
                "CREATE EDGE testToTwoE from "
                    + result.get(1).getIdentity()
                    + " to "
                    + result.get(0).getIdentity()))
        .execute();

    List<OIdentifiable> resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 2);

    database
        .command(new OCommandSQL("DELETE EDGE testToTwoE to " + result.get(0).getIdentity()))
        .execute();

    resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 1);

    database.command(new OCommandSQL("DELETE FROM testToOneE unsafe")).execute();
    database.command(new OCommandSQL("DELETE FROM testToTwoE unsafe")).execute();
    int deleted = database.command(new OCommandSQL("DELETE VERTEX testToV")).execute();
  }

  public void testDropClassVandEwithUnsafe() {
    database.command(new OCommandSQL("CREATE CLASS SuperE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS SuperV extends V")).execute();

    OIdentifiable v1 =
        database.command(new OCommandSQL("create vertex SuperV set name = 'Luca'")).execute();
    OIdentifiable v2 =
        database.command(new OCommandSQL("create vertex SuperV set name = 'Mark'")).execute();
    database
        .command(
            new OCommandSQL(
                "CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity()))
        .execute();

    try {
      database.command(new OCommandSQL("DROP CLASS SuperV")).execute();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      database.command(new OCommandSQL("DROP CLASS SuperE")).execute();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      database.command(new OCommandSQL("DROP CLASS SuperV unsafe")).execute();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(false);
    }

    try {
      database.command(new OCommandSQL("DROP CLASS SuperE UNSAFE")).execute();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(false);
    }
  }

  public void testDropClassVandEwithDeleteElements() {
    database.command(new OCommandSQL("CREATE CLASS SuperE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS SuperV extends V")).execute();

    OIdentifiable v1 =
        database.command(new OCommandSQL("create vertex SuperV set name = 'Luca'")).execute();
    OIdentifiable v2 =
        database.command(new OCommandSQL("create vertex SuperV set name = 'Mark'")).execute();
    database
        .command(
            new OCommandSQL(
                "CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity()))
        .execute();

    try {
      database.command(new OCommandSQL("DROP CLASS SuperV")).execute();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      database.command(new OCommandSQL("DROP CLASS SuperE")).execute();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    int deleted = database.command(new OCommandSQL("DELETE VERTEX SuperV")).execute();

    try {
      database.command(new OCommandSQL("DROP CLASS SuperV")).execute();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(false);
    }

    try {
      database.command(new OCommandSQL("DROP CLASS SuperE")).execute();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(false);
    }
  }

  public void testFromInString() {
    database.command(new OCommandSQL("CREATE CLASS FromInStringE extends E")).execute();
    database.command(new OCommandSQL("CREATE CLASS FromInStringV extends V")).execute();

    OIdentifiable v1 =
        database
            .command(new OCommandSQL("create vertex FromInStringV set name = ' from '"))
            .execute();
    OIdentifiable v2 =
        database
            .command(new OCommandSQL("create vertex FromInStringV set name = ' FROM '"))
            .execute();
    OIdentifiable v3 =
        database
            .command(new OCommandSQL("create vertex FromInStringV set name = ' TO '"))
            .execute();

    database
        .command(
            new OCommandSQL(
                "create edge FromInStringE from " + v1.getIdentity() + " to " + v2.getIdentity()))
        .execute();
    database
        .command(
            new OCommandSQL(
                "create edge FromInStringE from " + v1.getIdentity() + " to " + v3.getIdentity()))
        .execute();

    List<OIdentifiable> result =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT expand(out()[name = ' FROM ']) FROM FromInStringV"));
    Assert.assertEquals(result.size(), 1);

    result =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT expand(in()[name = ' from ']) FROM FromInStringV"));
    Assert.assertEquals(result.size(), 2);

    result =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "SELECT expand(out()[name = ' TO ']) FROM FromInStringV"));
    Assert.assertEquals(result.size(), 1);
  }

  public void testDeleteVertexWithReturn() {
    OIdentifiable v1 =
        database.command(new OCommandSQL("create vertex V set returning = true")).execute();

    List<OIdentifiable> v2s =
        database
            .command(new OCommandSQL("delete vertex V return before where returning = true"))
            .execute();

    Assert.assertEquals(v2s.size(), 1);
    Assert.assertTrue(v2s.contains(v1));
  }
}
