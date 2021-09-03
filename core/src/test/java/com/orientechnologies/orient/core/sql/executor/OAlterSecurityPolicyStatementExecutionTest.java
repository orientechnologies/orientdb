package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicy;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OAlterSecurityPolicyStatementExecutionTest {
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
    OCreateDatabaseUtil.createDatabase("test", orient, OCreateDatabaseUtil.TYPE_MEMORY);
    this.db = orient.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    orient.drop("test");
    this.db = null;
  }

  @Test
  public void testPlain() {
    db.command("CREATE SECURITY POLICY foo").close();

    db.command("ALTER SECURITY POLICY foo SET READ = (name = 'foo')").close();

    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();
    OSecurityPolicy policy = security.getSecurityPolicy((ODatabaseSession) db, "foo");
    Assert.assertNotNull(policy);
    Assert.assertNotNull("foo", policy.getName());
    Assert.assertEquals("name = 'foo'", policy.getReadRule().toString());
    Assert.assertNull(policy.getCreateRule());
    Assert.assertNull(policy.getBeforeUpdateRule());
    Assert.assertNull(policy.getAfterUpdateRule());
    Assert.assertNull(policy.getDeleteRule());
    Assert.assertNull(policy.getExecuteRule());

    db.command("ALTER SECURITY POLICY foo REMOVE READ").close();

    policy = security.getSecurityPolicy((ODatabaseSession) db, "foo");
    Assert.assertNotNull(policy);
    Assert.assertNull(policy.getReadRule());
  }
}
