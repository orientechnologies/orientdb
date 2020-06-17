package com.orientechnologies.orient.object.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.object.db.entity.NestedContainer;
import com.orientechnologies.orient.object.db.entity.NestedContent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 17/07/17. */
public class NestedCollectionsTest {

  private ODatabaseObject database;

  @Before
  public void before() {
    database = new OObjectDatabaseTx("memory:" + NestedCollectionsTest.class.getSimpleName());
    database.create();
    database.getEntityManager().registerEntityClass(NestedContainer.class);
    database.getEntityManager().registerEntityClass(NestedContent.class);
  }

  @Test
  public void testNestedCollections() {

    NestedContainer container = new NestedContainer("first");
    NestedContainer saved = database.save(container);

    assertEquals(1, saved.getFoo().size());
    assertEquals(3, saved.getFoo().get("key-1").size());
  }

  @After
  public void after() {
    database.drop();
  }
}
