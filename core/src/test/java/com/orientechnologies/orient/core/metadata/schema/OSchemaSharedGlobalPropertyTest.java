package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OSchemaException;
import org.junit.Test;

public class OSchemaSharedGlobalPropertyTest extends BaseMemoryDatabase {

  @Test
  public void testGlobalPropertyCreate() {

    OSchema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("testaasd", OType.SHORT, 100);
    OGlobalProperty prop = schema.getGlobalPropertyById(100);
    assertEquals(prop.getName(), "testaasd");
    assertEquals(prop.getId(), (Integer) 100);
    assertEquals(prop.getType(), OType.SHORT);
  }

  @Test
  public void testGlobalPropertyCreateDoubleSame() {

    OSchema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("test", OType.SHORT, 200);
    schema.createGlobalProperty("test", OType.SHORT, 200);
  }

  @Test(expected = OSchemaException.class)
  public void testGlobalPropertyCreateDouble() {

    OSchema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("test", OType.SHORT, 201);
    schema.createGlobalProperty("test1", OType.SHORT, 201);
  }
}
