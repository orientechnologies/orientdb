package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 11/03/16. */
public class ODocumentTrackingNestedCollectionsTest {

  private ODatabaseDocument db;

  @Before
  public void before() {
    db =
        new ODatabaseDocumentTx(
            "memory:" + ODocumentTrackingNestedCollectionsTest.class.getSimpleName());
    db.create();
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testTrackingNestedSet() {

    ORID orid;
    ODocument document = new ODocument();
    Set objects = new HashSet();

    document.field("objects", objects);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    objects = document.field("objects");
    Set subObjects = new HashSet();
    objects.add(subObjects);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    orid = document.getIdentity();
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();

    ODocument nestedDoc = new ODocument();
    subObjects.add(nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.getLocalCache().clear();

    document = db.load(orid);
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();

    assertTrue(!subObjects.isEmpty());
  }

  @Test
  public void testChangesValuesNestedTrackingSet() {

    ODocument document = new ODocument();
    Set objects = new HashSet();

    document.field("objects", objects);
    Set subObjects = new HashSet();
    objects.add(subObjects);

    ODocument nestedDoc = new ODocument();
    subObjects.add(nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();
    subObjects.add("one");

    assertTrue(document.isDirty());
    OMultiValueChangeTimeLine<Object, Object> nestedTimiline =
        ((OTrackedMultiValue<Object, Object>) subObjects).getTimeLine();
    assertEquals(1, nestedTimiline.getMultiValueChangeEvents().size());
    List<OMultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        nestedTimiline.getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getValue());
  }

  @Test
  public void testChangesValuesNestedTrackingList() {

    ODocument document = new ODocument();
    List objects = new ArrayList();

    document.field("objects", objects);
    List subObjects = new ArrayList();
    objects.add(subObjects);

    ODocument nestedDoc = new ODocument();
    subObjects.add(nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    objects = document.field("objects");
    subObjects = (List) objects.iterator().next();
    subObjects.add("one");
    subObjects.add(new ODocument());

    assertTrue(document.isDirty());
    List<OMultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        ((OTrackedMultiValue<Object, Object>) subObjects).getTimeLine().getMultiValueChangeEvents();
    assertEquals(1, multiValueChangeEvents.get(0).getKey());
    assertEquals("one", multiValueChangeEvents.get(0).getValue());
    assertEquals(2, multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof ODocument);
  }

  @Test
  public void testChangesValuesNestedTrackingMap() {

    ODocument document = new ODocument();
    Map objects = new HashMap();

    document.field("objects", objects);
    Map subObjects = new HashMap();
    objects.put("first", subObjects);

    ODocument nestedDoc = new ODocument();
    subObjects.put("one", nestedDoc);

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    objects = document.field("objects");
    subObjects = (Map) objects.values().iterator().next();
    subObjects.put("one", "String");
    subObjects.put("two", new ODocument());

    assertTrue(document.isDirty());
    List<OMultiValueChangeEvent<Object, Object>> multiValueChangeEvents =
        ((OTrackedMultiValue<Object, Object>) subObjects).getTimeLine().getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getKey());
    assertEquals("String", multiValueChangeEvents.get(0).getValue());
    assertEquals("two", multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof ODocument);
  }
}
