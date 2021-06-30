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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TrackedSetTest {
  @Test
  public void testAddOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    trackedSet.enableTracking(doc);
    OMultiValueChangeEvent<Object, Object> event =
        new OMultiValueChangeEvent<Object, Object>(
            OMultiValueChangeEvent.OChangeType.ADD, "value1", "value1", null);
    trackedSet.add("value1");
    Assert.assertEquals(event, trackedSet.getTimeLine().getMultiValueChangeEvents().get(0));
    Assert.assertTrue(trackedSet.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddTwo() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    doc.setProperty("tracked", trackedSet);
    trackedSet.add("value1");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testAddThree() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    trackedSet.enableTracking(doc);
    trackedSet.addInternal("value1");

    Assert.assertFalse(trackedSet.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testAddFour() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);

    trackedSet.add("value1");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.disableTracking(doc);
    trackedSet.enableTracking(doc);

    trackedSet.add("value1");
    Assert.assertFalse(trackedSet.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.enableTracking(doc);
    trackedSet.remove("value2");
    OMultiValueChangeEvent<Object, Object> event =
        new OMultiValueChangeEvent<Object, Object>(
            OMultiValueChangeEvent.OChangeType.REMOVE, "value2", null, "value2");
    Assert.assertEquals(trackedSet.getTimeLine().getMultiValueChangeEvents().get(0), event);
    Assert.assertTrue(trackedSet.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationTwo() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    doc.setProperty("tracked", trackedSet);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.remove("value2");
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testRemoveNotificationFour() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());
    trackedSet.disableTracking(doc);
    trackedSet.enableTracking(doc);
    trackedSet.remove("value5");
    Assert.assertFalse(trackedSet.isModified());
    Assert.assertFalse(doc.isDirty());
  }

  @Test
  public void testClearOne() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final List<OMultiValueChangeEvent<String, String>> firedEvents = new ArrayList<>();
    firedEvents.add(
        new OMultiValueChangeEvent<String, String>(
            OMultiValueChangeEvent.OChangeType.REMOVE, "value1", null, "value1"));
    firedEvents.add(
        new OMultiValueChangeEvent<String, String>(
            OMultiValueChangeEvent.OChangeType.REMOVE, "value2", null, "value2"));
    firedEvents.add(
        new OMultiValueChangeEvent<String, String>(
            OMultiValueChangeEvent.OChangeType.REMOVE, "value3", null, "value3"));

    trackedSet.enableTracking(doc);
    trackedSet.clear();

    Assert.assertEquals(firedEvents, trackedSet.getTimeLine().getMultiValueChangeEvents());
    Assert.assertTrue(trackedSet.isModified());
    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testClearThree() {
    final ODocument doc = new ODocument();
    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");

    ORecordInternal.unsetDirty(doc);
    Assert.assertFalse(doc.isDirty());

    trackedSet.clear();

    Assert.assertTrue(doc.isDirty());
  }

  @Test
  public void testReturnOriginalState() {
    final ODocument doc = new ODocument();

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    trackedSet.add("value1");
    trackedSet.add("value2");
    trackedSet.add("value3");
    trackedSet.add("value4");
    trackedSet.add("value5");

    final Set<String> original = new HashSet<String>(trackedSet);
    trackedSet.enableTracking(doc);
    trackedSet.add("value6");
    trackedSet.remove("value2");
    trackedSet.remove("value5");
    trackedSet.add("value7");
    trackedSet.add("value8");
    trackedSet.remove("value7");
    trackedSet.add("value9");
    trackedSet.add("value10");

    Assert.assertEquals(
        original,
        trackedSet.returnOriginalState(
            (List) trackedSet.getTimeLine().getMultiValueChangeEvents()));
  }

  /** Test that {@link OTrackedSet} is serialised correctly. */
  @Test
  public void testSetSerialization() throws Exception {

    class NotSerializableDocument extends ODocument {
      private static final long serialVersionUID = 1L;

      private void writeObject(ObjectOutputStream oos) throws IOException {
        throw new NotSerializableException();
      }
    }

    final OTrackedSet<String> beforeSerialization =
        new OTrackedSet<String>(new NotSerializableDocument());
    beforeSerialization.add("firstVal");
    beforeSerialization.add("secondVal");

    final OMemoryStream memoryStream = new OMemoryStream();
    ObjectOutputStream out = new ObjectOutputStream(memoryStream);
    out.writeObject(beforeSerialization);
    out.close();

    final ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(memoryStream.copy()));
    @SuppressWarnings("unchecked")
    final Set<String> afterSerialization = (Set<String>) input.readObject();

    Assert.assertEquals(afterSerialization.size(), beforeSerialization.size());
    Assert.assertTrue(beforeSerialization.containsAll(afterSerialization));
  }

  @Test
  public void testStackOverflowOnRecursion() {
    final ODocument doc = new ODocument();
    final OTrackedSet<ODocument> trackedSet = new OTrackedSet<>(doc);
    trackedSet.add(doc);
  }
}
