package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicy;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OAlterSecurityPolicyStatementExecutionTest extends BaseMemoryDatabase {

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
