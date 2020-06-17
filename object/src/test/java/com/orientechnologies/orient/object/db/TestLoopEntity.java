package com.orientechnologies.orient.object.db;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.object.db.entity.LoopEntity;
import org.junit.Test;

/** Created by tglman on 09/05/16. */
public class TestLoopEntity {

  @Test
  public void testLoop() {
    ODatabaseObject object =
        new OObjectDatabaseTx("memory:" + TestLoopEntity.class.getSimpleName());
    object.create();
    try {

      object.getEntityManager().registerEntityClasses(LoopEntity.class, true);

      assertTrue(object.getMetadata().getSchema().existsClass("LoopEntity"));
      assertTrue(object.getMetadata().getSchema().existsClass("LoopEntityLink"));
    } finally {
      object.drop();
    }
  }
}
