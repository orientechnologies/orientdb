package com.orientechnologies.orient.core.db.record;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.orientechnologies.common.types.ORef;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test
public class TrackedMapTest {
	public void testPutOne() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> map = new OTrackedMap<String>(doc);
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		map.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.ADD);
				Assert.assertNull(event.getOldValue());
				Assert.assertEquals(event.getKey(), "key1");
				Assert.assertEquals(event.getValue(), "value1");

				changed.value = true;
			}
		});

		map.put("key1", "value1");

		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testPutTwo() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> map = new OTrackedMap<String>(doc);
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		map.put("key1", "value1");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		map.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.UPDATE);
				Assert.assertEquals(event.getOldValue(), "value1");
				Assert.assertEquals(event.getKey(), "key1");
				Assert.assertEquals(event.getValue(), "value2");

				changed.value = true;
			}
		});

		map.put("key1", "value2");

		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testPutThree() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> map = new OTrackedMap<String>(doc);
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		map.put("key1", "value1");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		map.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				changed.value = true;
			}
		});

		map.put("key1", "value1");

		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testPutFour() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> map = new OTrackedMap<String>(doc);
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		map.put("key1", "value1");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		map.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				changed.value = true;
			}
		});

		map.put("key1", "value1");

		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testPutFive() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> map = new OTrackedMap<String>(doc);

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		map.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
		final ORef<Boolean> changed = new ORef<Boolean>(false);

		map.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				changed.value = true;
			}
		});

		map.put("key1", "value1");

		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testRemoveOne() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> map = new OTrackedMap<String>(doc);

		map.put("key1", "value1");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		map.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.REMOVE);
				Assert.assertEquals(event.getOldValue(), "value1");
				Assert.assertEquals(event.getKey(), "key1");
				Assert.assertEquals(event.getValue(), null);

				changed.value = true;
			}
		});

		map.remove("key1");

		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testRemoveTwo() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> map = new OTrackedMap<String>(doc);

		map.put("key1", "value1");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		map.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				changed.value = true;
			}
		});

		map.remove("key2");

		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testRemoveThree() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> map = new OTrackedMap<String>(doc);

		map.put("key1", "value1");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		map.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
		final ORef<Boolean> changed = new ORef<Boolean>(false);

		map.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				changed.value = true;
			}
		});

		map.remove("key1");

		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testClearOne() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> trackedMap = new OTrackedMap<String>(doc);

		trackedMap.put("key1", "value1");
		trackedMap.put("key2", "value2");
		trackedMap.put("key3", "value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final Set<OMultiValueChangeEvent<Object, String>> firedEvents = new HashSet<OMultiValueChangeEvent<Object, String>>();
		firedEvents.add(new OMultiValueChangeEvent<Object, String>(OMultiValueChangeEvent.OChangeType.REMOVE, "key1", null, "value1"));
		firedEvents.add(new OMultiValueChangeEvent<Object, String>(OMultiValueChangeEvent.OChangeType.REMOVE, "key2", null, "value2"));
		firedEvents.add(new OMultiValueChangeEvent<Object, String>(OMultiValueChangeEvent.OChangeType.REMOVE, "key3", null, "value3"));

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedMap.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				if (!firedEvents.remove(event))
					Assert.fail();

				changed.value = true;
			}
		});

		trackedMap.clear();

		Assert.assertEquals(firedEvents.size(), 0);
		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testClearTwo() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> trackedMap = new OTrackedMap<String>(doc);

		trackedMap.put("key1", "value1");
		trackedMap.put("key2", "value2");
		trackedMap.put("key3", "value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);
		trackedMap.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		trackedMap.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				changed.value = true;
			}
		});

		trackedMap.clear();

		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testClearThree() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> trackedMap = new OTrackedMap<String>(doc);

		trackedMap.put("key1", "value1");
		trackedMap.put("key2", "value2");
		trackedMap.put("key3", "value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedMap.clear();

		Assert.assertTrue(doc.isDirty());
	}

	public void testReturnOriginalStateOne() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> trackedMap = new OTrackedMap<String>(doc);
		trackedMap.put("key1", "value1");
		trackedMap.put("key2", "value2");
		trackedMap.put("key3", "value3");
		trackedMap.put("key4", "value4");
		trackedMap.put("key5", "value5");
		trackedMap.put("key6", "value6");
		trackedMap.put("key7", "value7");

		final Map<Object, String> original = new HashMap<Object, String>(trackedMap);

		final List<OMultiValueChangeEvent<Object, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Object, String>>();

		trackedMap.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				firedEvents.add(event);
			}
		});

		trackedMap.put("key8", "value8");
		trackedMap.put("key9", "value9");
		trackedMap.put("key2", "value10");
		trackedMap.put("key11", "value11");
		trackedMap.remove("key5");
		trackedMap.remove("key5");
		trackedMap.put("key3", "value12");
		trackedMap.remove("key8");
		trackedMap.remove("key3");

		Assert.assertEquals(trackedMap.returnOriginalState(firedEvents), original);

	}

	public void testReturnOriginalStateTwo() {
		final ODocument doc = new ODocument();

		final OTrackedMap<String> trackedMap = new OTrackedMap<String>(doc);
		trackedMap.put("key1", "value1");
		trackedMap.put("key2", "value2");
		trackedMap.put("key3", "value3");
		trackedMap.put("key4", "value4");
		trackedMap.put("key5", "value5");
		trackedMap.put("key6", "value6");
		trackedMap.put("key7", "value7");

		final Map<Object, String> original = new HashMap<Object, String>(trackedMap);

		final List<OMultiValueChangeEvent<Object, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Object, String>>();

		trackedMap.addChangeListener(new OMultiValueChangeListener<Object, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
				firedEvents.add(event);
			}
		});

		trackedMap.put("key8", "value8");
		trackedMap.put("key9", "value9");
		trackedMap.put("key2", "value10");
		trackedMap.put("key11", "value11");
		trackedMap.remove("key5");
		trackedMap.remove("key5");
		trackedMap.clear();
		trackedMap.put("key3", "value12");
		trackedMap.remove("key8");
		trackedMap.remove("key3");

		Assert.assertEquals(trackedMap.returnOriginalState(firedEvents), original);

	}

	/**
	 * Test that {@link OTrackedMap} is serialised correctly.
	 */
	@Test
	public void testMapSerialization() throws Exception {
		ODocument doc = new ODocument();

		OTrackedMap<String> beforeSerialization = new OTrackedMap<String>(doc);
		beforeSerialization.put(0, "firstVal");
		beforeSerialization.put(1, "secondVal");

		final OMemoryStream memoryStream = new OMemoryStream();
		ObjectOutputStream out = new ObjectOutputStream(memoryStream);
		out.writeObject(beforeSerialization);
		out.close();

		final ObjectInputStream input = new ObjectInputStream(new OMemoryInputStream(memoryStream.copy()));
		@SuppressWarnings("unchecked")
		final OTrackedMap<String> afterSerialization = (OTrackedMap<String>) input.readObject();

		Assert.assertEquals(afterSerialization.size(), beforeSerialization.size(), "Map size");
		for (int i = 0; i < afterSerialization.size(); i++) {
			Assert.assertEquals(afterSerialization.get(i), beforeSerialization.get(i));
		}
	}
}
