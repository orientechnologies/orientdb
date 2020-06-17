package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateLinkStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateLinkStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testBasic() throws Exception {
    db.command("create class Basic1").close();
    db.command("create class Basic2").close();

    db.command("insert into Basic1 set pk = 'pkb1_1', fk = 'pkb2_1'").close();
    db.command("insert into Basic1 set pk = 'pkb1_2', fk = 'pkb2_2'").close();

    db.command("insert into Basic2 set pk = 'pkb2_1'").close();
    db.command("insert into Basic2 set pk = 'pkb2_2'").close();

    db.command("CREATE LINK theLink type link FROM Basic1.fk TO Basic2.pk ").close();

    OResultSet result = db.query("select pk, theLink.pk as other from Basic1 order by pk");
    Assert.assertTrue(result.hasNext());

    OResult item = result.next();
    Object otherKey = item.getProperty("other");
    Assert.assertNotNull(otherKey);

    Assert.assertEquals(otherKey, "pkb2_1");

    Assert.assertTrue(result.hasNext());

    item = result.next();
    otherKey = item.getProperty("other");
    Assert.assertEquals(otherKey, "pkb2_2");
  }

  @Test
  public void testInverse() throws Exception {
    db.command("create class Inverse1").close();
    db.command("create class Inverse2").close();

    db.command("insert into Inverse1 set pk = 'pkb1_1', fk = 'pkb2_1'").close();
    db.command("insert into Inverse1 set pk = 'pkb1_2', fk = 'pkb2_2'").close();
    db.command("insert into Inverse1 set pk = 'pkb1_3', fk = 'pkb2_2'").close();

    db.command("insert into Inverse2 set pk = 'pkb2_1'").close();
    db.command("insert into Inverse2 set pk = 'pkb2_2'").close();

    db.command("CREATE LINK theLink TYPE LINKSET FROM Inverse1.fk TO Inverse2.pk INVERSE").close();

    OResultSet result = db.query("select pk, theLink.pk as other from Inverse2 order by pk");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();

    Object otherKeys = item.getProperty("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).get(0), "pkb1_1");

    Assert.assertTrue(result.hasNext());
    item = result.next();
    otherKeys = item.getProperty("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).size(), 2);
    Assert.assertTrue(((List) otherKeys).contains("pkb1_2"));
    Assert.assertTrue(((List) otherKeys).contains("pkb1_3"));
  }
}
