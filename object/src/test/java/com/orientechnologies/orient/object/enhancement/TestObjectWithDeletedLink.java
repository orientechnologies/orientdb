package com.orientechnologies.orient.object.enhancement;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;

public class TestObjectWithDeletedLink {

  private ODatabaseObject db;

  @BeforeTest
  public void before() {
    db = new OObjectDatabaseTx("memory:" + TestObjectWithDeletedLink.class.getName());
    db.create();
    db.getEntityManager().registerEntityClass(SimpleSelfRef.class);
  }

  @AfterTest
  public void after() {
    db.drop();
  }

  @Test
  public void testDeletedLink() {
    SimpleSelfRef ob1 = new SimpleSelfRef();
    ob1.setName("hobby one ");
    SimpleSelfRef ob2 = new SimpleSelfRef();
    ob2.setName("2");
    ob1.setFriend(ob2);

    ob1 = db.save(ob1);

    ob1 = db.reload(ob1, "", true);
    ob2 = ob1.getFriend();
    Assert.assertNotNull(ob1.getFriend());
    db.delete(ob2);

    ob1 = db.reload(ob1, "", true);
    Assert.assertNull(ob1.getFriend());
  }

}
