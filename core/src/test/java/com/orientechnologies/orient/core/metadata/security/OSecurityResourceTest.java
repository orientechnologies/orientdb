package com.orientechnologies.orient.core.metadata.security;

import org.junit.Assert;
import org.junit.Test;

public class OSecurityResourceTest {

  @Test
  public void testParse() {
    Assert.assertEquals(
        OSecurityResourceClass.ALL_CLASSES, OSecurityResource.parseResource("database.class.*"));
    Assert.assertEquals(
        OSecurityResourceProperty.ALL_PROPERTIES,
        OSecurityResource.parseResource("database.class.*.*"));
    Assert.assertEquals(
        OSecurityResourceCluster.ALL_CLUSTERS,
        OSecurityResource.parseResource("database.cluster.*"));
    Assert.assertEquals(
        OSecurityResourceFunction.ALL_FUNCTIONS,
        OSecurityResource.parseResource("database.function.*"));
    Assert.assertTrue(
        OSecurityResource.parseResource("database.class.Person") instanceof OSecurityResourceClass);
    Assert.assertEquals(
        "Person",
        ((OSecurityResourceClass) OSecurityResource.parseResource("database.class.Person"))
            .getClassName());
    Assert.assertTrue(
        OSecurityResource.parseResource("database.class.Person.name")
            instanceof OSecurityResourceProperty);
    Assert.assertEquals(
        "Person",
        ((OSecurityResourceProperty) OSecurityResource.parseResource("database.class.Person.name"))
            .getClassName());
    Assert.assertEquals(
        "name",
        ((OSecurityResourceProperty) OSecurityResource.parseResource("database.class.Person.name"))
            .getPropertyName());
    Assert.assertTrue(
        OSecurityResource.parseResource("database.class.*.name")
            instanceof OSecurityResourceProperty);
    Assert.assertTrue(
        OSecurityResource.parseResource("database.cluster.person")
            instanceof OSecurityResourceCluster);
    Assert.assertEquals(
        "person",
        ((OSecurityResourceCluster) OSecurityResource.parseResource("database.cluster.person"))
            .getClusterName());
    Assert.assertTrue(
        OSecurityResource.parseResource("database.function.foo")
            instanceof OSecurityResourceFunction);
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.BYPASS_RESTRICTED,
        OSecurityResource.parseResource("database.bypassRestricted"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.COMMAND, OSecurityResource.parseResource("database.command"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.COMMAND_GREMLIN,
        OSecurityResource.parseResource("database.command.gremlin"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.COPY, OSecurityResource.parseResource("database.copy"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.CREATE, OSecurityResource.parseResource("database.create"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.DB, OSecurityResource.parseResource("database"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.DROP, OSecurityResource.parseResource("database.drop"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.EXISTS, OSecurityResource.parseResource("database.exists"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.FREEZE, OSecurityResource.parseResource("database.freeze"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.PASS_THROUGH,
        OSecurityResource.parseResource("database.passthrough"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.RELEASE, OSecurityResource.parseResource("database.release"));
    Assert.assertEquals(
        OSecurityResourceDatabaseOp.HOOK_RECORD,
        OSecurityResource.parseResource("database.hook.record"));
    Assert.assertNotEquals(
        OSecurityResourceDatabaseOp.DB, OSecurityResource.parseResource("database.command"));

    Assert.assertEquals(
        OSecurityResourceServerOp.SERVER, OSecurityResource.parseResource("server"));
    Assert.assertEquals(
        OSecurityResourceServerOp.REMOVE, OSecurityResource.parseResource("server.remove"));
    Assert.assertEquals(
        OSecurityResourceServerOp.STATUS, OSecurityResource.parseResource("server.status"));
    Assert.assertEquals(
        OSecurityResourceServerOp.ADMIN, OSecurityResource.parseResource("server.admin"));

    try {
      OSecurityResource.parseResource("database.class.person.foo.bar");
      Assert.fail();
    } catch (Exception e) {
    }
    try {
      OSecurityResource.parseResource("database.cluster.person.foo");
      Assert.fail();
    } catch (Exception e) {
    }
    try {
      OSecurityResource.parseResource("database.function.foo.bar");
      Assert.fail();
    } catch (Exception e) {
    }
    try {
      OSecurityResource.parseResource("database.foo");
      Assert.fail();
    } catch (Exception e) {
    }
    try {
      OSecurityResource.parseResource("server.foo");
      Assert.fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void testCache() {
    OSecurityResource person = OSecurityResource.getInstance("database.class.Person");
    OSecurityResource person2 = OSecurityResource.getInstance("database.class.Person");
    Assert.assertTrue(person == person2);
  }
}
