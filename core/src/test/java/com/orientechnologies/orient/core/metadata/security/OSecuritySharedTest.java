package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import org.junit.Assert;
import org.junit.Test;

public class OSecuritySharedTest extends BaseMemoryDatabase {

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
