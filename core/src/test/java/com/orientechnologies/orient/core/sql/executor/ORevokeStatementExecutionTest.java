package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.*;
import org.junit.*;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ORevokeStatementExecutionTest {
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
  public void testSimple() {
    ORole testRole = db.getMetadata().getSecurity().createRole("testRole", OSecurityRole.ALLOW_MODES.DENY_ALL_BUT);
    Assert.assertFalse(testRole.allow(ORule.ResourceGeneric.SERVER, "server", ORole.PERMISSION_EXECUTE));
    db.command("GRANT execute on server.remove to testRole");
    testRole = db.getMetadata().getSecurity().getRole("testRole");
    Assert.assertTrue(testRole.allow(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE));
    db.command("REVOKE execute on server.remove from testRole");
    testRole = db.getMetadata().getSecurity().getRole("testRole");
    Assert.assertFalse(testRole.allow(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE));
  }


  @Test
  public void testRemovePolicy(){
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    Assert.assertEquals("testPolicy", security.getSecurityPolicies(db, security.getRole(db, "reader")).get("database.class.Person").getName());
    db.command("REVOKE POLICY ON database.class.Person FROM reader").close();
    Assert.assertNull(security.getSecurityPolicies(db, security.getRole(db, "reader")).get("database.class.Person"));
  }

}
