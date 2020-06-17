package com.orientechnologies.orient.object.enhancement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.CascadeType;
import javax.persistence.Embedded;
import javax.persistence.OneToMany;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author JN <a href="mailto:jn@brain-activit.com">Julian Neuhaus</a>
 * @author Nathan Brown (anecdotesoftware--at--gmail.com)
 * @since 18.08.2014
 */
public class OObjectProxyMethodHandlerTest {
  private OObjectDatabaseTx databaseTx;

  private Map<String, Object> fieldsAndThereDefaultValue;

  @Before
  public void setUp() throws Exception {
    databaseTx = new OObjectDatabaseTx("memory:OObjectEnumLazyListTest");
    databaseTx.create();

    databaseTx.getEntityManager().registerEntityClass(EntityWithDifferentFieldTypes.class);
    databaseTx.getEntityManager().registerEntityClass(EmbeddedType1.class);
    databaseTx.getEntityManager().registerEntityClass(EmbeddedType2.class);
    databaseTx.getEntityManager().registerEntityClass(EntityWithEmbeddedFields.class);

    fieldsAndThereDefaultValue = new HashMap<String, Object>();
    fieldsAndThereDefaultValue.put("byteField", Byte.valueOf("0"));
    fieldsAndThereDefaultValue.put("shortField", Short.valueOf("0"));
    fieldsAndThereDefaultValue.put("intField", 0);
    fieldsAndThereDefaultValue.put("longField", 0L);
    fieldsAndThereDefaultValue.put("floatField", 0.0f);
    fieldsAndThereDefaultValue.put("doubleField", 0.0d);
    fieldsAndThereDefaultValue.put("stringField", null);
    fieldsAndThereDefaultValue.put("booleanField", false);
    fieldsAndThereDefaultValue.put("objectField", null);
  }

  @After
  public void tearDown() {
    databaseTx.drop();
  }

  @Test
  public void reloadTestForMapsInTarget() {
    EntityWithDifferentFieldTypes targetObject =
        this.databaseTx.newInstance(EntityWithDifferentFieldTypes.class);
    EntityWithDifferentFieldTypes childObject =
        this.databaseTx.newInstance(EntityWithDifferentFieldTypes.class);

    Map<String, String> map = new HashMap<String, String>();
    map.put("key", "value");

    targetObject.setStringStringMap(map);
    Map<String, String> map2 = new HashMap<String, String>();
    map2.put("key", "value");

    Map<String, String> map3 = childObject.getStringStringMap();
    map2.put("key", "value");

    targetObject.getListOfEntityWithDifferentFieldTypes().add(childObject);

    targetObject = this.databaseTx.save(targetObject);

    for (String key : targetObject.getStringStringMap().keySet()) {
      assertTrue(key.equals("key"));
    }
    targetObject.getStringStringMap().put("key2", "value2");

    childObject.getStringStringMap().put("key3", "value3");
    targetObject = this.databaseTx.save(targetObject);
    //        targetObject = this.databaseTx.load(targetObject);

    targetObject.getStringStringMap().get("key");
    targetObject.getStringStringMap2().get("key2");

    targetObject.getStringStringMap2().put("key3", "value3");
  }

  @Test
  public void reloadTestForListsInTarget() {
    EntityWithDifferentFieldTypes targetObject = new EntityWithDifferentFieldTypes();

    List<EntityWithDifferentFieldTypes> entitieList =
        new ArrayList<EntityWithDifferentFieldTypes>();

    EntityWithDifferentFieldTypes listObject1 = new EntityWithDifferentFieldTypes();
    EntityWithDifferentFieldTypes listObject2 = new EntityWithDifferentFieldTypes();

    listObject1.setBooleanField(true);
    listObject1.setByteField(Byte.MIN_VALUE);
    listObject1.setDoubleField(1.1);
    listObject1.setFloatField(1f);
    listObject1.setIntField(13);
    listObject1.setLongField(10);
    listObject1.setShortField(Short.MIN_VALUE);
    listObject1.setStringField("TEST2");

    listObject2.setBooleanField(true);
    listObject2.setByteField(Byte.MIN_VALUE);
    listObject2.setDoubleField(1.1);
    listObject2.setFloatField(1f);
    listObject2.setIntField(13);
    listObject2.setLongField(10);
    listObject2.setShortField(Short.MIN_VALUE);
    listObject2.setStringField("TEST2");

    entitieList.add(listObject1);
    entitieList.add(listObject2);

    targetObject.setListOfEntityWithDifferentFieldTypes(entitieList);

    targetObject = this.databaseTx.save(targetObject);

    for (EntityWithDifferentFieldTypes entity :
        targetObject.getListOfEntityWithDifferentFieldTypes()) {
      assertTrue(entity.isBooleanField() == true);
      assertTrue(entity.getByteField() == Byte.MIN_VALUE);
      assertTrue(entity.getDoubleField() == 1.1);
      assertTrue(entity.getFloatField() == 1f);
      assertTrue(entity.getIntField() == 13);
      assertTrue(entity.getLongField() == 10);
      assertTrue(entity.getObjectField() == null);
      assertTrue(entity.getShortField() == Short.MIN_VALUE);
      assertTrue("TEST2".equals(entity.getStringField()));

      entity.setBooleanField(false);
      entity.setByteField(Byte.MAX_VALUE);
      entity.setDoubleField(3.1);
      entity.setFloatField(2f);
      entity.setIntField(15);
      entity.setLongField(11);
      entity.setShortField(Short.MAX_VALUE);
      entity.setStringField("TEST3");
      entity.setObjectField(new EntityWithDifferentFieldTypes());
    }

    for (EntityWithDifferentFieldTypes entity :
        targetObject.getListOfEntityWithDifferentFieldTypes()) {
      this.databaseTx.reload(entity);
      assertTrue(entity.isBooleanField() == true);
      assertTrue(entity.getByteField() == Byte.MIN_VALUE);
      assertTrue(entity.getDoubleField() == 1.1);
      assertTrue(entity.getFloatField() == 1f);
      assertTrue(entity.getIntField() == 13);
      assertTrue(entity.getLongField() == 10);
      assertTrue(entity.getObjectField() == null);
      assertTrue(entity.getShortField() == Short.MIN_VALUE);
      assertTrue("TEST2".equals(entity.getStringField()));
    }
  }

  @Test
  public void getDefaultValueForFieldTest() {
    OObjectProxyMethodHandler handler = new OObjectProxyMethodHandler(null);

    Method m = null;

    try {
      m = handler.getClass().getDeclaredMethod("getDefaultValueForField", Field.class);
      m.setAccessible(true);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }

    assertTrue(m != null);

    EntityWithDifferentFieldTypes testEntity = new EntityWithDifferentFieldTypes();

    for (String fieldName : fieldsAndThereDefaultValue.keySet()) {
      Field field = null;

      try {
        field = testEntity.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Unexpected exception");
      }

      assertTrue(field != null);

      try {
        Object defaultValue = m.invoke(handler, field);
        Object expectedValue = fieldsAndThereDefaultValue.get(fieldName);

        if (expectedValue == null) assertTrue(defaultValue == null);
        else assertTrue(expectedValue.equals(defaultValue));
      } catch (Exception e) {
        e.printStackTrace();
        fail("Unexpected exception");
      }
    }
  }

  @Test
  public void testEntityWithEmbeddedFieldDetachingAllWithoutError() throws Exception {
    EntityWithEmbeddedFields entity = new EntityWithEmbeddedFields();
    entity.setEmbeddedType1(new EmbeddedType1());
    entity.setEmbeddedType2(new EmbeddedType2());
    EntityWithEmbeddedFields saved = databaseTx.save(entity);
    databaseTx.detachAll(saved, true);
  }

  public static class EmbeddedType1 {}

  public static class EmbeddedType2 {}

  public static class EntityWithEmbeddedFields {
    @Embedded private EmbeddedType1 _embeddedType1;
    @Embedded private EmbeddedType2 _embeddedType2;

    public EntityWithEmbeddedFields() {}

    public EmbeddedType1 getEmbeddedType1() {
      return _embeddedType1;
    }

    public void setEmbeddedType1(EmbeddedType1 embeddedType1) {
      _embeddedType1 = embeddedType1;
    }

    public EmbeddedType2 getEmbeddedType2() {
      return _embeddedType2;
    }

    public void setEmbeddedType2(EmbeddedType2 embeddedType2) {
      _embeddedType2 = embeddedType2;
    }
  }

  public class EntityWithDifferentFieldTypes {
    private byte byteField;
    private short shortField;
    private int intField;
    private long longField;
    private float floatField;
    private double doubleField;
    private String stringField;
    private boolean booleanField;
    private EntityWithDifferentFieldTypes objectField;
    private Map<String, String> stringStringMap = new HashMap<String, String>();
    private Map<String, String> stringStringMap2 = new HashMap<String, String>();

    @OneToMany(cascade = CascadeType.ALL)
    private List<EntityWithDifferentFieldTypes> listOfEntityWithDifferentFieldTypes;

    public EntityWithDifferentFieldTypes() {
      super();
      this.listOfEntityWithDifferentFieldTypes = new ArrayList<EntityWithDifferentFieldTypes>();
    }

    public byte getByteField() {
      return byteField;
    }

    public void setByteField(byte byteField) {
      this.byteField = byteField;
    }

    public short getShortField() {
      return shortField;
    }

    public void setShortField(short shortField) {
      this.shortField = shortField;
    }

    public int getIntField() {
      return intField;
    }

    public void setIntField(int intField) {
      this.intField = intField;
    }

    public long getLongField() {
      return longField;
    }

    public void setLongField(long longField) {
      this.longField = longField;
    }

    public float getFloatField() {
      return floatField;
    }

    public void setFloatField(float floatField) {
      this.floatField = floatField;
    }

    public double getDoubleField() {
      return doubleField;
    }

    public void setDoubleField(double doubleField) {
      this.doubleField = doubleField;
    }

    public String getStringField() {
      return stringField;
    }

    public void setStringField(String stringField) {
      this.stringField = stringField;
    }

    public boolean isBooleanField() {
      return booleanField;
    }

    public void setBooleanField(boolean booleanField) {
      this.booleanField = booleanField;
    }

    public EntityWithDifferentFieldTypes getObjectField() {
      return objectField;
    }

    public void setObjectField(EntityWithDifferentFieldTypes objectField) {
      this.objectField = objectField;
    }

    public List<EntityWithDifferentFieldTypes> getListOfEntityWithDifferentFieldTypes() {
      return listOfEntityWithDifferentFieldTypes;
    }

    public void setListOfEntityWithDifferentFieldTypes(
        List<EntityWithDifferentFieldTypes> listOfEntityWithDifferentFieldTypes) {
      this.listOfEntityWithDifferentFieldTypes = listOfEntityWithDifferentFieldTypes;
    }

    public Map<String, String> getStringStringMap() {
      return stringStringMap;
    }

    public void setStringStringMap(Map<String, String> stringStringMap) {
      this.stringStringMap = stringStringMap;
    }

    public Map<String, String> getStringStringMap2() {
      return stringStringMap2;
    }

    public void setStringStringMap2(Map<String, String> stringStringMap2) {
      this.stringStringMap2 = stringStringMap2;
    }
  }
}
