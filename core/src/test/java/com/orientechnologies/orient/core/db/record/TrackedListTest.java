package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TrackedListTest {
  @Test
  public void testAddNotificationOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.enableTracking(doc);
    OMultiValueChangeEvent<Object, Object> event =
        new OMultiValueChangeEvent<Object, Object>(
            OMultiValueChangeEvent.OChangeType.ADD, 0, "value1", null);
    trackedList.add("value1");

    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedList.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddNotificationTwo() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");

    trackedList.disableTracking(doc);
    trackedList.enableTracking(doc);
    OMultiValueChangeEvent<Object, Object> event =
        new OMultiValueChangeEvent<Object, Object>(
            OMultiValueChangeEvent.OChangeType.ADD, 2, "value3", null);

    trackedList.add("value3");
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedList.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddNotificationThree() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    trackedList.add("value1");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddNotificationFour() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.disableTracking(doc);
    Assert.assertFalse(trackedList.isModified());
    trackedList.enableTracking(doc);

    trackedList.addInternal("value3");
    Assert.assertFalse(trackedList.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testAddAllNotificationOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    final List<String> valuesToAdd = new ArrayList<String>();
    valuesToAdd.add("value1");
    valuesToAdd.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final List<OMultiValueChangeEvent<Integer, String>> firedEvents =
        new ArrayList<OMultiValueChangeEvent<Integer, String>>();
    firedEvents.add(
        new OMultiValueChangeEvent<Integer, String>(
            OMultiValueChangeEvent.OChangeType.ADD, 0, "value1"));
    firedEvents.add(
        new OMultiValueChangeEvent<Integer, String>(
            OMultiValueChangeEvent.OChangeType.ADD, 1, "value3"));
    trackedList.enableTracking(doc);
    trackedList.addAll(valuesToAdd);

    Assert.assertEquals(firedEvents, trackedList.getTimeLine().getMultiValueChangeEvents());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddAllNotificationTwo() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    final List<String> valuesToAdd = new ArrayList<String>();
    valuesToAdd.add("value1");
    valuesToAdd.add("value3");

    trackedList.addAll(valuesToAdd);

    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddAllNotificationThree() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    final List<String> valuesToAdd = new ArrayList<String>();
    valuesToAdd.add("value1");
    valuesToAdd.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.disableTracking(doc);
    trackedList.enableTracking(doc);
    for (String e : valuesToAdd) {
      trackedList.addInternal(e);
    }

    Assert.assertFalse(trackedList.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testAddIndexNotificationOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.disableTracking(doc);
    trackedList.enableTracking(doc);

    OMultiValueChangeEvent<Integer, String> event =
        new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.ADD, 1, "value3", null);

    trackedList.add(1, "value3");
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedList.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddIndexNotificationTwo() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    doc.setProperty("aa", trackedList);
    trackedList.add("value1");
    trackedList.add("value2");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.add(1, "value3");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testSetNotificationOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.enableTracking(doc);
    OMultiValueChangeEvent<Object, Object> event =
        new OMultiValueChangeEvent<Object, Object>(
            OMultiValueChangeEvent.OChangeType.UPDATE, 1, "value4", "value2");
    trackedList.set(1, "value4");
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedList.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testSetNotificationTwo() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.set(1, "value4");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.enableTracking(doc);
    trackedList.remove("value2");
    OMultiValueChangeEvent<Integer, String> event =
        new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.REMOVE, 1, null, "value2");
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationTwo() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.remove("value2");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationFour() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.disableTracking(doc);

    trackedList.remove("value4");
    Assert.assertFalse(trackedList.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testRemoveIndexOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedList.enableTracking(doc);

    trackedList.remove(1);
    OMultiValueChangeEvent<Object, Object> event =
        new OMultiValueChangeEvent<Object, Object>(
            OMultiValueChangeEvent.OChangeType.REMOVE, 1, null, "value2");
    Assert.assertTrue(trackedList.isModified());
    Assert.assertEquals(event, trackedList.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testClearOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final List<OMultiValueChangeEvent<Integer, String>> firedEvents =
        new ArrayList<OMultiValueChangeEvent<Integer, String>>();
    firedEvents.add(
        new OMultiValueChangeEvent<Integer, String>(
            OMultiValueChangeEvent.OChangeType.REMOVE, 2, null, "value3"));
    firedEvents.add(
        new OMultiValueChangeEvent<Integer, String>(
            OMultiValueChangeEvent.OChangeType.REMOVE, 1, null, "value2"));
    firedEvents.add(
        new OMultiValueChangeEvent<Integer, String>(
            OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, "value1"));
    trackedList.enableTracking(doc);

    trackedList.clear();
    Assert.assertEquals(firedEvents, trackedList.getTimeLine().getMultiValueChangeEvents());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testClearTwo() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    doc.setProperty("tracked", trackedList);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedList.clear();
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testReturnOriginalStateOne() {
    final ODocument doc = new ODocument();

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");
    trackedList.add("value4");
    trackedList.add("value5");

    final List<String> original = new ArrayList<String>(trackedList);
    trackedList.enableTracking(doc);
    trackedList.add("value6");
    trackedList.add("value7");
    trackedList.set(2, "value10");
    trackedList.add(1, "value8");
    trackedList.add(1, "value8");
    trackedList.remove(3);
    trackedList.remove("value7");
    trackedList.add(0, "value9");
    trackedList.add(0, "value9");
    trackedList.add(0, "value9");
    trackedList.add(0, "value9");
    trackedList.remove("value9");
    trackedList.remove("value9");
    trackedList.add(4, "value11");

    Assert.assertEquals(
        original,
        trackedList.returnOriginalState(
            (List) trackedList.getTimeLine().getMultiValueChangeEvents()));
  }

  @Test
  public void testReturnOriginalStateTwo() {
    final ODocument doc = new ODocument();

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    trackedList.add("value1");
    trackedList.add("value2");
    trackedList.add("value3");
    trackedList.add("value4");
    trackedList.add("value5");

    final List<String> original = new ArrayList<String>(trackedList);
    trackedList.enableTracking(doc);
    trackedList.add("value6");
    trackedList.add("value7");
    trackedList.set(2, "value10");
    trackedList.add(1, "value8");
    trackedList.remove(3);
    trackedList.clear();
    trackedList.remove("value7");
    trackedList.add(0, "value9");
    trackedList.add("value11");
    trackedList.add(0, "value12");
    trackedList.add("value12");

    Assert.assertEquals(
        original,
        trackedList.returnOriginalState(
            (List) trackedList.getTimeLine().getMultiValueChangeEvents()));
  }

  /** Test that {@link OTrackedList} is serialised correctly. */
  @Test
  public void testSerialization() throws Exception {

    class NotSerializableDocument extends ODocument {
      private static final long serialVersionUID = 1L;

      private void writeObject(ObjectOutputStream oos) throws IOException {
        throw new NotSerializableException();
      }
    }

    final OTrackedList<String> beforeSerialization =
        new OTrackedList<String>(new NotSerializableDocument());
    beforeSerialization.add("firstVal");
    beforeSerialization.add("secondVal");

    final OMemoryStream memoryStream = new OMemoryStream();
    ObjectOutputStream out = new ObjectOutputStream(memoryStream);
    out.writeObject(beforeSerialization);
    out.close();

    final ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(memoryStream.copy()));
    @SuppressWarnings("unchecked")
    final List<String> afterSerialization = (List<String>) input.readObject();

    Assert.assertEquals(afterSerialization.size(), beforeSerialization.size());
    for (int i = 0; i < afterSerialization.size(); i++) {
      Assert.assertEquals(afterSerialization.get(i), beforeSerialization.get(i));
    }
  }
}
