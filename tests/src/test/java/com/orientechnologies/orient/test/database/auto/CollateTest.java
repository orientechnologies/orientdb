package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
public class CollateTest {

  private ODatabaseDocument database;

  @BeforeMethod
  public void beforeMethod() {
    database.open("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() {
    database.close();
  }

  @Parameters(value = "url")
  public CollateTest(String iURL) {
    database = new ODatabaseDocumentTx(iURL);
  }

  @Test
  public void testCaseInsensitiveQuery() {
    assertEquals(database.query(new OSQLSynchQuery<Object>("select from ouser where name = 'AdMiN'")).size(), 1);
    assertEquals(database.query(new OSQLSynchQuery<Object>("select from ouser where name = 'admin'")).size(), 1);
    assertEquals(database.query(new OSQLSynchQuery<Object>("select from ouser where name like 'admin'")).size(), 1);
    assertEquals(database.query(new OSQLSynchQuery<Object>("select from ouser where name like 'AdMIN'")).size(), 1);
    assertEquals(database.query(new OSQLSynchQuery<Object>("select from ouser where name like '%dm%'")).size(), 1);
    assertEquals(database.query(new OSQLSynchQuery<Object>("select from ouser where name like '%MI%'")).size(), 1);
    assertEquals(database.query(new OSQLSynchQuery<Object>("select from ouser where any() like '%dM%'")).size(), 1);
    assertEquals(database.query(new OSQLSynchQuery<Object>("select from ouser where all() like '%MI%'")).size(), 0);
  }

}
