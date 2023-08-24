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
    database.command("CREATE CLASS testFromToOneE extends E").close();
    database.command("CREATE CLASS testFromToTwoE extends E").close();
    database.command("CREATE CLASS testFromToV extends V").close();

    database.command("create vertex testFromToV set name = 'Luca'").close();
    database.command("create vertex testFromToV set name = 'Luca'").close();

    List<OIdentifiable> result =
        database.query(new OSQLSynchQuery<ODocument>("select from testFromToV"));

    database
        .command(
            "CREATE EDGE testFromToOneE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    database
        .command(
            "CREATE EDGE testFromToTwoE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();

    List<OIdentifiable> resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 2);

    database
        .command(
            "DELETE EDGE testFromToTwoE from "
                + result.get(1).getIdentity()
                + " to"
                + result.get(0).getIdentity())
        .close();

    resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 1);

    database.command("DELETE FROM testFromToOneE unsafe").close();
    database.command("DELETE FROM testFromToTwoE unsafe").close();
    int deleted = database.command(new OCommandSQL("DELETE VERTEX testFromToV")).execute();
  }

  public void testDeleteFrom() {
    database.command("CREATE CLASS testFromOneE extends E").close();
    database.command("CREATE CLASS testFromTwoE extends E").close();
    database.command("CREATE CLASS testFromV extends V").close();

    database.command("create vertex testFromV set name = 'Luca'").close();
    database.command("create vertex testFromV set name = 'Luca'").close();

    List<OIdentifiable> result =
        database.query(new OSQLSynchQuery<ODocument>("select from testFromV"));

    database
        .command(
            "CREATE EDGE testFromOneE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    database
        .command(
            "CREATE EDGE testFromTwoE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();

    List<OIdentifiable> resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 2);

    try {
      database.command("DELETE EDGE testFromTwoE from " + result.get(1).getIdentity()).close();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 1);

    database.command("DELETE FROM testFromOneE unsafe").close();
    database.command("DELETE FROM testFromTwoE unsafe").close();
    int deleted = database.command(new OCommandSQL("DELETE VERTEX testFromV")).execute();
  }

  public void testDeleteTo() {
    database.command("CREATE CLASS testToOneE extends E").close();
    database.command("CREATE CLASS testToTwoE extends E").close();
    database.command("CREATE CLASS testToV extends V").close();

    database.command("create vertex testToV set name = 'Luca'").close();
    database.command("create vertex testToV set name = 'Luca'").close();

    List<OIdentifiable> result =
        database.query(new OSQLSynchQuery<ODocument>("select from testToV"));

    database
        .command(
            "CREATE EDGE testToOneE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    database
        .command(
            "CREATE EDGE testToTwoE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();

    List<OIdentifiable> resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 2);

    database.command("DELETE EDGE testToTwoE to " + result.get(0).getIdentity()).close();

    resultTwo =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select expand(outE()) from " + result.get(1).getIdentity()));
    Assert.assertEquals(resultTwo.size(), 1);

    database.command("DELETE FROM testToOneE unsafe").close();
    database.command("DELETE FROM testToTwoE unsafe").close();
    int deleted = database.command(new OCommandSQL("DELETE VERTEX testToV")).execute();
  }

  public void testDropClassVandEwithUnsafe() {
    database.command("CREATE CLASS SuperE extends E").close();
    database.command("CREATE CLASS SuperV extends V").close();

    OIdentifiable v1 =
        database.command(new OCommandSQL("create vertex SuperV set name = 'Luca'")).execute();
    OIdentifiable v2 =
        database.command(new OCommandSQL("create vertex SuperV set name = 'Mark'")).execute();
    database
        .command("CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity())
        .close();

    try {
      database.command("DROP CLASS SuperV").close();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      database.command("DROP CLASS SuperE").close();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      database.command("DROP CLASS SuperV unsafe").close();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(false);
    }

    try {
      database.command("DROP CLASS SuperE UNSAFE").close();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(false);
    }
  }

  public void testDropClassVandEwithDeleteElements() {
    database.command("CREATE CLASS SuperE extends E").close();
    database.command("CREATE CLASS SuperV extends V").close();

    OIdentifiable v1 =
        database.command(new OCommandSQL("create vertex SuperV set name = 'Luca'")).execute();
    OIdentifiable v2 =
        database.command(new OCommandSQL("create vertex SuperV set name = 'Mark'")).execute();
    database
        .command("CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity())
        .close();

    try {
      database.command("DROP CLASS SuperV").close();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      database.command("DROP CLASS SuperE").close();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    int deleted = database.command(new OCommandSQL("DELETE VERTEX SuperV")).execute();

    try {
      database.command("DROP CLASS SuperV").close();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(false);
    }

    try {
      database.command("DROP CLASS SuperE").close();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(false);
    }
  }

  public void testFromInString() {
    database.command("CREATE CLASS FromInStringE extends E").close();
    database.command("CREATE CLASS FromInStringV extends V").close();

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
        .command("create edge FromInStringE from " + v1.getIdentity() + " to " + v2.getIdentity())
        .close();
    database
        .command("create edge FromInStringE from " + v1.getIdentity() + " to " + v3.getIdentity())
        .close();

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
