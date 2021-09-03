package com.orientechnologies.orient.object.db;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author JN <a href="mailto:jn@brain-activit.com">Julian Neuhaus</a>
 * @since 21.08.2014
 */
public class OObjectLazyMapTest {
  private final int idOfRootEntity = 0;
  private final int idOfFirstMapEntry = 1;
  private final int idOfSecondMapEntry = 2;
  private final int invalidId = 3;
  private OObjectDatabaseTx databaseTx;

  @Before
  public void setUp() throws Exception {
    databaseTx = new OObjectDatabaseTx("memory:OObjectLazyMapTest");
    databaseTx.create();

    databaseTx.getEntityManager().registerEntityClass(EntityWithMap.class);
  }

  @After
  public void tearDown() {
    databaseTx.drop();
  }

  @Test
  public void isEmptyTest() {
    Map<String, EntityWithMap> testMap = getMapWithPersistedEntries();

    assertTrue(testMap.size() > 0);
    testMap.clear();
    assertTrue(testMap.size() == 0);

    assertTrue(testMap.get(String.valueOf(idOfFirstMapEntry)) == null);
  }

  @Test
  public void getContainsValueTest() {
    Map<String, EntityWithMap> testMap = getMapWithPersistedEntries();

    assertFalse(testMap.containsValue(null));
    assertFalse(testMap.containsValue(String.valueOf(invalidId)));

    assertTrue(testMap.containsValue(testMap.get(String.valueOf(idOfFirstMapEntry))));
    assertTrue(testMap.containsValue(testMap.get(String.valueOf(idOfSecondMapEntry))));
  }

  @Test
  public void getContainsKeyTest() {
    Map<String, EntityWithMap> testMap = getMapWithPersistedEntries();

    assertFalse(testMap.containsKey(null));
    assertFalse(testMap.containsKey(String.valueOf(invalidId)));

    // should fail because the keys will be automatically converted to string
    assertFalse(testMap.containsKey(idOfFirstMapEntry));

    assertTrue(testMap.containsKey(String.valueOf(idOfFirstMapEntry)));
    assertTrue(testMap.containsKey(String.valueOf(idOfSecondMapEntry)));
  }

  @Test
  public void getTest() {
    Map<String, EntityWithMap> testMap = getMapWithPersistedEntries();

    assertTrue(testMap.get(String.valueOf(invalidId)) == null);

    // should fail because the keys will be automatically converted to string
    try {
      testMap.get(idOfFirstMapEntry);
      fail("Expected ClassCastException");
    } catch (ClassCastException e) {
    }

    assertTrue(testMap.get(String.valueOf(idOfFirstMapEntry)) != null);
    assertTrue(testMap.get(String.valueOf(idOfSecondMapEntry)) != null);
  }

  @Test
  public void getOrDefaultTest() {
    Object toCast = getMapWithPersistedEntries();
    assertTrue(toCast instanceof OObjectLazyMap);

    @SuppressWarnings({"unchecked", "rawtypes"})
    OObjectLazyMap<EntityWithMap> testMap = (OObjectLazyMap) toCast;

    assertTrue(testMap.getClass() == OObjectLazyMap.class);
    assertTrue(testMap.getOrDefault(String.valueOf(idOfFirstMapEntry), null) != null);
    assertTrue(testMap.getOrDefault(String.valueOf(idOfSecondMapEntry), null) != null);
    assertTrue(testMap.getOrDefault(String.valueOf(invalidId), null) == null);
    assertTrue(
        testMap.getOrDefault(
                String.valueOf(invalidId), testMap.get(String.valueOf(idOfFirstMapEntry)))
            == testMap.get(String.valueOf(idOfFirstMapEntry)));
  }

  private Map<String, EntityWithMap> getMapWithPersistedEntries() {
    EntityWithMap toStore = new EntityWithMap();
    toStore.setId(idOfRootEntity);

    EntityWithMap mapElement1 = new EntityWithMap();
    mapElement1.setId(idOfFirstMapEntry);

    EntityWithMap mapElement2 = new EntityWithMap();
    mapElement2.setId(idOfSecondMapEntry);

    Map<String, EntityWithMap> mapToStore = new HashMap<String, OObjectLazyMapTest.EntityWithMap>();
    mapToStore.put(String.valueOf(mapElement1.getId()), mapElement1);
    mapToStore.put(String.valueOf(mapElement2.getId()), mapElement2);

    toStore.setMap(mapToStore);

    EntityWithMap fromDb = this.databaseTx.save(toStore);

    assertTrue(fromDb != null);
    assertTrue(fromDb.getMap() != null);

    Map<String, EntityWithMap> testMap = fromDb.getMap();

    assertTrue(testMap != null);

    return testMap;
  }

  public class EntityWithMap {
    private int id;
    private Map<String, EntityWithMap> map;

    public Map<String, EntityWithMap> getMap() {
      return map;
    }

    public void setMap(Map<String, EntityWithMap> map) {
      this.map = map;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }
  }
}
