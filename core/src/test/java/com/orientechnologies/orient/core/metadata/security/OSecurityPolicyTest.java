package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OSecurityPolicyTest {
  static OrientDB orientDB;
  ODatabaseSession db;

  @BeforeClass
  public static void beforeClass() {
    orientDB =
        new OrientDB(
            "plocal:.",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
  }

  @Before
  public void before() {
    orientDB.execute(
        "create database "
            + getClass().getSimpleName()
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    this.db =
        orientDB.open(getClass().getSimpleName(), "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    OResultSet rs =
        db.query(
            "select from " + OSecurityPolicy.class.getSimpleName() + " WHERE name = ?", "test");
    Assert.assertFalse(rs.hasNext());
    rs.close();
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    OSecurityPolicy policy = security.createSecurityPolicy(db, "test");

    rs =
        db.query(
            "select from " + OSecurityPolicy.class.getSimpleName() + " WHERE name = ?", "test");
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
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule("name = 'create'");
    policy.setReadRule("name = 'read'");
    policy.setBeforeUpdateRule("name = 'beforeUpdate'");
    policy.setAfterUpdateRule("name = 'afterUpdate'");
    policy.setDeleteRule("name = 'delete'");
    policy.setExecuteRule("name = 'execute'");

    security.saveSecurityPolicy(db, policy);
    OSecurityPolicy readPolicy = security.getSecurityPolicy(db, "test");
    Assert.assertNotNull(policy);
    Assert.assertEquals("name = 'create'", readPolicy.getCreateRule());
    Assert.assertEquals("name = 'read'", readPolicy.getReadRule());
    Assert.assertEquals("name = 'beforeUpdate'", readPolicy.getBeforeUpdateRule());
    Assert.assertEquals("name = 'afterUpdate'", readPolicy.getAfterUpdateRule());
    Assert.assertEquals("name = 'delete'", readPolicy.getDeleteRule());
    Assert.assertEquals("name = 'execute'", readPolicy.getExecuteRule());
  }

  @Test
  public void testInvalidPredicates() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "test");
    try {
      policy.setCreateRule("foo bar");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      policy.setReadRule("foo bar");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      policy.setBeforeUpdateRule("foo bar");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      policy.setAfterUpdateRule("foo bar");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      policy.setDeleteRule("foo bar");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      policy.setExecuteRule("foo bar");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
  }

  @Test
  public void testAddPolicyToRole() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule("1 = 1");
    policy.setBeforeUpdateRule("1 = 2");
    policy.setActive(true);
    security.saveSecurityPolicy(db, policy);

    ORole reader = security.getRole(db, "reader");
    String resource = "database.class.Person";
    security.setSecurityPolicy(db, reader, resource, policy);

    ORID policyRid = policy.getElement().getIdentity();
    try (OResultSet rs = db.query("select from ORole where name = 'reader'")) {
      Map<String, OIdentifiable> rolePolicies = rs.next().getProperty("policies");
      OIdentifiable id = rolePolicies.get(resource);
      Assert.assertEquals(id.getIdentity(), policyRid);
    }

    OSecurityPolicy policy2 = security.getSecurityPolicy(db, reader, resource);
    Assert.assertNotNull(policy2);
    Assert.assertEquals(policy2.getIdentity(), policyRid);
  }

  @Test
  public void testRemovePolicyToRole() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule("1 = 1");
    policy.setBeforeUpdateRule("1 = 2");
    policy.setActive(true);
    security.saveSecurityPolicy(db, policy);

    ORole reader = security.getRole(db, "reader");
    String resource = "database.class.Person";
    security.setSecurityPolicy(db, reader, resource, policy);

    security.removeSecurityPolicy(db, reader, resource);
    Assert.assertNull(security.getSecurityPolicy(db, reader, resource));
  }
}
