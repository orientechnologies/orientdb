package com.orientechnologies.orient.object.enhancement;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 18.05.12
 */
public class OObjectEntitySerializerTest {

  private OObjectDatabaseTx databaseTx;

  @Before
  public void setUp() throws Exception {
    databaseTx = new OObjectDatabaseTx("memory:OObjectEntitySerializerTest");
    databaseTx.create();

    databaseTx.getEntityManager().registerEntityClass(ExactEntity.class);
  }

  @After
  public void tearDown() {
    databaseTx.drop();
  }

  @Test
  public void testCallbacksHierarchy() {
    ExactEntity entity = new ExactEntity();
    databaseTx.save(entity);

    assertTrue(entity.callbackExecuted());
  }

  @Test
  public void testCallbacksHierarchyUpdate() {
    ExactEntity entity = new ExactEntity();
    entity = databaseTx.save(entity);

    entity.reset();
    databaseTx.save(entity);
    assertTrue(entity.callbackExecuted());
  }
}
