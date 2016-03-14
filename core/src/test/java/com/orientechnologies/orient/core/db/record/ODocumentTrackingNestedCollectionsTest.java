package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ONestedMultiValueChangeEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 11/03/16.
 */
public class ODocumentTrackingNestedCollectionsTest {

  private ODatabaseDocument db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + ODocumentTrackingNestedCollectionsTest.class.getSimpleName());
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
    document.save();

    objects = document.field("objects");
    Set subObjects = new HashSet();
    objects.add(subObjects);

    document.save();

    orid = document.getIdentity();
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();

    ODocument nestedDoc = new ODocument();
    subObjects.add(nestedDoc);

    document.save();
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

    document.save();

    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();
    subObjects.add("one");

    OMultiValueChangeTimeLine<Object, Object> timeLine = document.getCollectionTimeLine("objects");

    assertEquals(1, timeLine.getMultiValueChangeEvents().size());
    assertTrue(timeLine.getMultiValueChangeEvents().get(0) instanceof ONestedMultiValueChangeEvent);
    ONestedMultiValueChangeEvent nesetedEvent = (ONestedMultiValueChangeEvent) timeLine.getMultiValueChangeEvents().get(0);
    assertEquals(1, nesetedEvent.getTimeLine().getMultiValueChangeEvents().size());
    List<OMultiValueChangeEvent<?, ?>> multiValueChangeEvents = nesetedEvent.getTimeLine().getMultiValueChangeEvents();
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

    document.save();

    objects = document.field("objects");
    subObjects = (List) objects.iterator().next();
    subObjects.add("one");
    subObjects.add(new ODocument());

    OMultiValueChangeTimeLine<Object, Object> timeLine = document.getCollectionTimeLine("objects");

    assertEquals(1, timeLine.getMultiValueChangeEvents().size());
    assertTrue(timeLine.getMultiValueChangeEvents().get(0) instanceof ONestedMultiValueChangeEvent);
    ONestedMultiValueChangeEvent nesetedEvent = (ONestedMultiValueChangeEvent) timeLine.getMultiValueChangeEvents().get(0);
    assertEquals(2, nesetedEvent.getTimeLine().getMultiValueChangeEvents().size());
    List<OMultiValueChangeEvent<?, ?>> multiValueChangeEvents = nesetedEvent.getTimeLine().getMultiValueChangeEvents();
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

    document.save();

    objects = document.field("objects");
    subObjects = (Map) objects.values().iterator().next();
    subObjects.put("one", "String");
    subObjects.put("two", new ODocument());

    OMultiValueChangeTimeLine<Object, Object> timeLine = document.getCollectionTimeLine("objects");

    assertEquals(1, timeLine.getMultiValueChangeEvents().size());
    assertTrue(timeLine.getMultiValueChangeEvents().get(0) instanceof ONestedMultiValueChangeEvent);
    ONestedMultiValueChangeEvent nesetedEvent = (ONestedMultiValueChangeEvent) timeLine.getMultiValueChangeEvents().get(0);
    assertEquals(2, nesetedEvent.getTimeLine().getMultiValueChangeEvents().size());
    List<OMultiValueChangeEvent<?, ?>> multiValueChangeEvents = nesetedEvent.getTimeLine().getMultiValueChangeEvents();
    assertEquals("one", multiValueChangeEvents.get(0).getKey());
    assertEquals("String", multiValueChangeEvents.get(0).getValue());
    assertEquals("two", multiValueChangeEvents.get(1).getKey());
    assertTrue(multiValueChangeEvents.get(1).getValue() instanceof ODocument);

  }

}
