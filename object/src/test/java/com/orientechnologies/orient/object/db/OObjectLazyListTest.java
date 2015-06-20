package com.orientechnologies.orient.object.db;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Anders Heintz on 20/06/15.
 */
public class OObjectLazyListTest {
  private OObjectDatabaseTx databaseTx;

  @BeforeClass protected void setUp() throws Exception {
    databaseTx = new OObjectDatabaseTx("memory:OObjectEnumLazyListTest");
    databaseTx.create();

    databaseTx.getEntityManager().registerEntityClass(EntityObject.class);
    databaseTx.getEntityManager().registerEntityClass(EntityObjectWithList.class);

  }

  @AfterClass protected void tearDown() {

    databaseTx.drop();
  }

  @Test public void positionTest() {
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
    assert (listObject.getEntityObjects().get(listObject.getEntityObjects().size()-1).getFieldValue().equals("NewObject2"));
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
    public EntityObjectWithList() {
    }

    private List<EntityObject> entityObjects = new ArrayList<EntityObject>();

    public List<EntityObject> getEntityObjects() {
      return entityObjects;
    }

    public void setEntityObjects(List<EntityObject> entityObjects) {
      this.entityObjects = entityObjects;
    }
  }

  private static class EntityObject {
    public EntityObject() {
    }

    private String fieldValue = null;

    public String getFieldValue() {
      return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
      this.fieldValue = fieldValue;
    }
  }

}
