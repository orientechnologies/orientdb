package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OSecuritySharedTest {

  static OrientDB orient;
  private ODatabaseSession db;

  @BeforeClass
  public static void beforeClass() {
    orient =
        new OrientDB(
            "plocal:.",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
  }

  @AfterClass
  public static void afterClass() {
    orient.close();
  }

  @Before
  public void before() {
    orient.execute(
        "create database "
            + "test"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    this.db = orient.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    orient.drop("test");
    this.db = null;
  }

  @Test
  public void testCreateSecurityPolicy() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    security.createSecurityPolicy(db, "testPolicy");
    Assert.assertNotNull(security.getSecurityPolicy(db, "testPolicy"));
  }

  @Test
  public void testDeleteSecurityPolicy() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    security.createSecurityPolicy(db, "testPolicy");
    security.deleteSecurityPolicy(db, "testPolicy");
    Assert.assertNull(security.getSecurityPolicy(db, "testPolicy"));
  }

  @Test
  public void testUpdateSecurityPolicy() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    Assert.assertTrue(security.getSecurityPolicy(db, "testPolicy").isActive());
    Assert.assertEquals("name = 'foo'", security.getSecurityPolicy(db, "testPolicy").getReadRule());
  }

  @Test
  public void testBindPolicyToRole() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);

    Assert.assertEquals(
        "testPolicy",
        security
            .getSecurityPolicies(db, security.getRole(db, "reader"))
            .get("database.class.Person")
            .getName());
  }

  @Test
  public void testUnbindPolicyFromRole() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    security.removeSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person");

    Assert.assertNull(
        security
            .getSecurityPolicies(db, security.getRole(db, "reader"))
            .get("database.class.Person"));
  }
}
