package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OSecurityEngineTest {

  static OrientDB orient;
  private ODatabaseSession db;
  private static String DB_NAME = "test";

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
            + DB_NAME
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    this.db = orient.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    orient.drop(DB_NAME);
    this.db = null;
  }

  @Test
  public void testAllClasses() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(true);
    policy.setReadRule("name = 'admin'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.*", policy);

    OBooleanExpression pred =
        OSecurityEngine.getPredicateForSecurityResource(
            db, (OSecurityShared) security, "database.class.Person", OSecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'admin'", pred.toString());
  }

  @Test
  public void testSingleClass() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    OBooleanExpression pred =
        OSecurityEngine.getPredicateForSecurityResource(
            db, (OSecurityShared) security, "database.class.Person", OSecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'foo'", pred.toString());
  }

  @Test
  public void testSuperclass() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");
    db.createClass("Employee", "Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    OBooleanExpression pred =
        OSecurityEngine.getPredicateForSecurityResource(
            db, (OSecurityShared) security, "database.class.Employee", OSecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'foo'", pred.toString());
  }

  @Test
  public void testSuperclass2() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");
    db.createClass("Employee", "Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    policy = security.createSecurityPolicy(db, "policy2");
    policy.setActive(true);
    policy.setReadRule("name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "admin"), "database.class.Employee", policy);

    OBooleanExpression pred =
        OSecurityEngine.getPredicateForSecurityResource(
            db, (OSecurityShared) security, "database.class.Employee", OSecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'bar'", pred.toString());
  }

  @Test
  public void testSuperclass3() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");
    db.createClass("Employee", "Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(true);
    policy.setReadRule("name = 'admin'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    policy = security.createSecurityPolicy(db, "policy2");
    policy.setActive(true);
    policy.setReadRule("name = 'bar' OR name = 'admin'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.*", policy);

    OBooleanExpression pred =
        OSecurityEngine.getPredicateForSecurityResource(
            db, (OSecurityShared) security, "database.class.Employee", OSecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'admin'", pred.toString());
  }

  @Test
  public void testTwoSuperclasses() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");
    db.createClass("Foo");
    db.createClass("Employee", "Person", "Foo");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    policy = security.createSecurityPolicy(db, "policy2");
    policy.setActive(true);
    policy.setReadRule("surname = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Foo", policy);

    OBooleanExpression pred =
        OSecurityEngine.getPredicateForSecurityResource(
            db, (OSecurityShared) security, "database.class.Employee", OSecurityPolicy.Scope.READ);

    Assert.assertTrue(
        "name = 'foo' AND surname = 'bar'".equals(pred.toString())
            || "surname = 'bar' AND name = 'foo'".equals(pred.toString()));
  }

  @Test
  public void testTwoRoles() {

    db.command(
        "Update OUser set roles = roles || (select from orole where name = 'reader') where name = 'admin'");
    db.close();
    db = orient.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    policy = security.createSecurityPolicy(db, "policy2");
    policy.setActive(true);
    policy.setReadRule("surname = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);

    OBooleanExpression pred =
        OSecurityEngine.getPredicateForSecurityResource(
            db, (OSecurityShared) security, "database.class.Person", OSecurityPolicy.Scope.READ);

    Assert.assertTrue(
        "name = 'foo' OR surname = 'bar'".equals(pred.toString())
            || "surname = 'bar' OR name = 'foo'".equals(pred.toString()));
  }

  @Test
  public void testRecordFiltering() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");
    OElement record1 = db.newElement("Person");
    record1.setProperty("name", "foo");
    record1.save();

    OElement record2 = db.newElement("Person");
    record2.setProperty("name", "bar");
    record2.save();

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    OBooleanExpression pred =
        OSecurityEngine.getPredicateForSecurityResource(
            db, (OSecurityShared) security, "database.class.Person", OSecurityPolicy.Scope.READ);

    Assert.assertTrue(OSecurityEngine.evaluateSecuirtyPolicyPredicate(db, pred, record1));
    Assert.assertFalse(OSecurityEngine.evaluateSecuirtyPolicyPredicate(db, pred, record2));
  }
}
