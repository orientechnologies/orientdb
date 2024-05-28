package com.orientechnologies.orient.object.enhancement;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 18.05.12
 */
public class OObjectEntitySerializerTest {

  private ODatabaseObject database;

  @Before
  public void setUp() throws Exception {
    database = new OObjectDatabaseTx("memory:OObjectEntitySerializerTest");
    database.create();

    database.getEntityManager().registerEntityClass(ExactEntity.class);
  }

  @After
  public void tearDown() {
    database.drop();
  }

  @Test
  public void testCallbacksHierarchy() {
    ExactEntity entity = new ExactEntity();
    database.save(entity);

    assertTrue(entity.callbackExecuted());
  }

  @Test
  public void testCallbacksHierarchyUpdate() {
    ExactEntity entity = new ExactEntity();
    entity = database.save(entity);

    entity.reset();
    database.save(entity);
    assertTrue(entity.callbackExecuted());
  }
}
