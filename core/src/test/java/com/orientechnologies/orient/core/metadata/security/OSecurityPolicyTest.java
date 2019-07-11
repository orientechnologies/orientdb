package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.*;

public class OSecurityPolicyTest {
  static OrientDB orientDB;
  ODatabaseSession db;


  @BeforeClass
  public static void beforeClass(){
    orientDB = new OrientDB("plocal:.", OrientDBConfig.defaultConfig());
  }

  @Before
  public void before(){
    orientDB.create(getClass().getSimpleName(), ODatabaseType.MEMORY);
    db = orientDB.open(getClass().getSimpleName(), "admin", "admin");
  }

  @AfterClass
  public static void afterClass(){
    orientDB.close();
    orientDB = null;
  }

  @After
  public void after(){
    db.close();
    orientDB.drop(getClass().getSimpleName());
  }

  @Test
  public void testSecurityPolicyCreate(){
    OResultSet rs = db.query("select from " + OSecurityPolicy.class.getSimpleName());
    Assert.assertFalse(rs.hasNext());
    rs.close();
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    OSecurityPolicy policy = security.createSecurityPolicy(db, "test");

    rs = db.query("select from " + OSecurityPolicy.class.getSimpleName());
    Assert.assertTrue(rs.hasNext());
    OResult item = rs.next();
    Assert.assertEquals("test", item.getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSecurityPolicyGet(){
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));
    security.createSecurityPolicy(db, "test");
    Assert.assertNotNull(security.getSecurityPolicy(db, "test"));
  }

  
}
