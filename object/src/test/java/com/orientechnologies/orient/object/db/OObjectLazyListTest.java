package com.orientechnologies.orient.object.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by Anders Heintz on 20/06/15. */
public class OObjectLazyListTest {
  private OObjectDatabaseTx databaseTx;
  private int count;

  @Before
  public void setUp() throws Exception {
    databaseTx = new OObjectDatabaseTx("memory:OObjectEnumLazyListTest");
    databaseTx.create();

    databaseTx.getEntityManager().registerEntityClass(EntityObject.class);
    databaseTx.getEntityManager().registerEntityClass(EntityObjectWithList.class);
  }

  @After
  public void tearDown() {

    databaseTx.drop();
  }

  @Test
  public void positionTest() {
    EntityObjectWithList listObject = getTestObject();

    listObject = databaseTx.save(listObject);

    EntityObject newObject = new EntityObject();
    newObject.setFieldValue("NewObject");
    EntityObject newObject2 = new EntityObject();
    newObject2.setFieldValue("NewObject2");
    listObject.getEntityObjects().add(0, newObject);
    listObject.getEntityObjects().add(listObject.getEntityObjects().size(), newObject2);

    listObject = databaseTx.save(listObject);

    assert (listObject.getEntityObjects().get(0).getFieldValue().equals("NewObject"));
    assert (listObject
        .getEntityObjects()
        .get(listObject.getEntityObjects().size() - 1)
        .getFieldValue()
        .equals("NewObject2"));
    listObject.getEntityObjects().stream()
        .forEach(
            (entityObject) -> {
              assertNotNull(entityObject);
            });
  }

  @Test
  public void stream() {
    EntityObjectWithList listObject = getTestObject();

    listObject = databaseTx.save(listObject);

    EntityObject newObject = new EntityObject();
    newObject.setFieldValue("NewObject");
    EntityObject newObject2 = new EntityObject();
    newObject2.setFieldValue("NewObject2");
    listObject.getEntityObjects().add(0, newObject);
    listObject.getEntityObjects().add(listObject.getEntityObjects().size(), newObject2);

    listObject = databaseTx.save(listObject);
    count = 0;
    listObject.getEntityObjects().stream()
        .forEach(
            (entityObject) -> {
              assertNotNull(entityObject);
              count++;
            });

    assertEquals(listObject.getEntityObjects().size(), count);
  }

  private EntityObjectWithList getTestObject() {

    EntityObject entityObject1 = new EntityObject();
    entityObject1.setFieldValue("Object 1");
    EntityObject entityObject2 = new EntityObject();
    entityObject2.setFieldValue("Object 1");
    EntityObject entityObject3 = new EntityObject();
    entityObject3.setFieldValue("Object 1");
    EntityObject entityObject4 = new EntityObject();
    entityObject4.setFieldValue("Object 1");
    EntityObject entityObject5 = new EntityObject();
    entityObject5.setFieldValue("Object 1");

    EntityObjectWithList listObject = new EntityObjectWithList();
    listObject.getEntityObjects().add(entityObject1);
    listObject.getEntityObjects().add(entityObject2);
    listObject.getEntityObjects().add(entityObject3);
    listObject.getEntityObjects().add(entityObject4);
    listObject.getEntityObjects().add(entityObject5);

    return listObject;
  }

  private static class EntityObjectWithList {
    private List<EntityObject> entityObjects = new ArrayList<EntityObject>();

    public EntityObjectWithList() {}

    public List<EntityObject> getEntityObjects() {
      return entityObjects;
    }

    public void setEntityObjects(List<EntityObject> entityObjects) {
      this.entityObjects = entityObjects;
    }
  }

  private static class EntityObject {
    private String fieldValue = null;

    public EntityObject() {}

    public String getFieldValue() {
      return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
      this.fieldValue = fieldValue;
    }
  }
}
