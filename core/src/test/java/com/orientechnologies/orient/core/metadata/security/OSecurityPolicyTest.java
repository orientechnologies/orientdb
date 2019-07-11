package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.*;

public class OSecurityPolicyTest {
  static OrientDB orientDB;
  ODatabaseSession db;


  @BeforeClass
  public static void beforeClass() {
    orientDB = new OrientDB("plocal:.", OrientDBConfig.defaultConfig());
  }

  @Before
  public void before() {
    orientDB.create(getClass().getSimpleName(), ODatabaseType.MEMORY);
    db = orientDB.open(getClass().getSimpleName(), "admin", "admin");
  }

  @AfterClass
  public static void afterClass() {
    orientDB.close();
    orientDB = null;
  }

  @After
  public void after() {
    db.close();
    orientDB.drop(getClass().getSimpleName());
  }

  @Test
  public void testSecurityPolicyCreate() {
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
  public void testSecurityPolicyGet() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));
    security.createSecurityPolicy(db, "test");
    Assert.assertNotNull(security.getSecurityPolicy(db, "test"));
  }

  @Test
  public void testValidPredicates() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));
    OSecurityPolicy policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule("name = 'create'");
    policy.setReadRule("name = 'read'");
    policy.setBeforeUpdateRule("name = 'beforeUpdate'");
    policy.setAfterUpdateRule("name = 'afterUpdate'");
    policy.setDeleteRule("name = 'delete'");
    policy.setExecuteRule("name = 'execute'");


    security.saveSecurityPolicy(db, policy);
    policy = security.getSecurityPolicy(db, "test");
    Assert.assertNotNull(policy);
    Assert.assertEquals("name = 'create'", policy.getCreateRule());
    Assert.assertEquals("name = 'read'", policy.getReadRule());
    Assert.assertEquals("name = 'beforeUpdate'", policy.getBeforeUpdateRule());
    Assert.assertEquals("name = 'afterUpdate'", policy.getAfterUpdateRule());
    Assert.assertEquals("name = 'delete'", policy.getDeleteRule());
    Assert.assertEquals("name = 'execute'", policy.getExecuteRule());
  }


  @Test
  public void testInvalidPredicates() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));
    OSecurityPolicy policy = security.createSecurityPolicy(db, "test");
    try{
      policy.setCreateRule("foo bar");
      Assert.fail();
    }catch(IllegalArgumentException ex){
    }
    try{
      policy.setReadRule("foo bar");
      Assert.fail();
    }catch(IllegalArgumentException ex){
    }
    try{
      policy.setBeforeUpdateRule("foo bar");
      Assert.fail();
    }catch(IllegalArgumentException ex){
    }
    try{
      policy.setAfterUpdateRule("foo bar");
      Assert.fail();
    }catch(IllegalArgumentException ex){
    }
    try{
      policy.setDeleteRule("foo bar");
      Assert.fail();
    }catch(IllegalArgumentException ex){
    }
    try{
      policy.setExecuteRule("foo bar");
      Assert.fail();
    }catch(IllegalArgumentException ex){
    }



  }
}
