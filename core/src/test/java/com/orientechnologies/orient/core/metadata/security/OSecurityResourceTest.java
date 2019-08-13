package com.orientechnologies.orient.core.metadata.security;

import org.junit.Assert;
import org.junit.Test;

public class OSecurityResourceTest {

  @Test
  public void testParse(){
    Assert.assertEquals(OSecurityResourceClass.ALL_CLASSES, OSecurityResource.parseResource("database.class.*"));
    Assert.assertEquals(OSecurityResourceProperty.ALL_PROPERTIES, OSecurityResource.parseResource("database.class.*.*"));
    Assert.assertEquals(OSecurityResourceCluster.ALL_CLUSTERS, OSecurityResource.parseResource("database.cluster.*"));
    Assert.assertEquals(OSecurityResourceFunction.ALL_FUNCTIONS, OSecurityResource.parseResource("database.function.*"));
    Assert.assertTrue(OSecurityResource.parseResource("database.class.Person") instanceof OSecurityResourceClass);
    Assert.assertTrue(OSecurityResource.parseResource("database.class.Person.name") instanceof OSecurityResourceProperty);
    Assert.assertTrue(OSecurityResource.parseResource("database.class.*.name") instanceof OSecurityResourceProperty);
    Assert.assertTrue(OSecurityResource.parseResource("database.cluster.person") instanceof OSecurityResourceCluster);
    Assert.assertTrue(OSecurityResource.parseResource("database.function.foo") instanceof OSecurityResourceFunction);
    try{
      OSecurityResource.parseResource("database.class.person.foo.bar");
      Assert.fail();
    }catch (Exception e){
    }
    try{
      OSecurityResource.parseResource("database.cluster.person.foo");
      Assert.fail();
    }catch (Exception e){
    }
    try{
      OSecurityResource.parseResource("database.function.foo.bar");
      Assert.fail();
    }catch (Exception e){
    }
  }
}
