package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import org.junit.*;

public class OSecurityEngineTest {

  static OrientDB orient;
  private ODatabaseSession db;

  @BeforeClass
  public static void beforeClass() {
    orient = new OrientDB("plocal:.", OrientDBConfig.defaultConfig());
  }

  @AfterClass
  public static void afterClass() {
    orient.close();
  }

  @Before
  public void before() {
    orient.create("test", ODatabaseType.MEMORY);
    this.db = orient.open("test", "admin", "admin");
  }

  @After
  public void after() {
    this.db.close();
    orient.drop("test");
    this.db = null;
  }

  @Test
  public void testSingleClass() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicy policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);


    OBooleanExpression pred = OSecurityEngine
            .getPredicateForSecurityResource(db, (OSecurityShared) security, "database.class.Person", OSecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'foo'", pred.toString());


  }
}
