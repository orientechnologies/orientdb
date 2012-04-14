package com.orientechnologies.orient.core.db.record;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.orientechnologies.common.types.ORef;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test
public class TrackedListTest {
	public void testAddNotificationOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.ADD);
				Assert.assertNull(event.getOldValue());
				Assert.assertEquals(event.getKey().intValue(), 0);
				Assert.assertEquals(event.getValue(), "value1");

				changed.value = true;
			}
		});

		trackedList.add("value1");
		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testAddNotificationTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.ADD);
				Assert.assertNull(event.getOldValue());
				Assert.assertEquals(event.getKey().intValue(), 2);
				Assert.assertEquals(event.getValue(), "value3");
				changed.value = true;
			}
		});

		trackedList.add("value3");
		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testAddNotificationThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		Assert.assertTrue(doc.isDirty());
	}

	public void testAddNotificationFour() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());
		trackedList.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				changed.value = true;
			}
		});

		trackedList.add("value3");
		Assert.assertEquals(changed.value, Boolean.FALSE);
		Assert.assertFalse(doc.isDirty());
	}

	public void testAddAllNotificationOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		final List<String> valuesToAdd = new ArrayList<String>();
		valuesToAdd.add("value1");
		valuesToAdd.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final List<OMultiValueChangeEvent<Integer, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Integer, String>>();
		firedEvents.add(new OMultiValueChangeEvent<Integer, String>(OMultiValueChangeEvent.OChangeType.ADD, 0, "value1"));
		firedEvents.add(new OMultiValueChangeEvent<Integer, String>(OMultiValueChangeEvent.OChangeType.ADD, 1, "value3"));

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				if (firedEvents.get(0).equals(event))
					firedEvents.remove(0);
				else
					Assert.fail();
			}
		});

		trackedList.addAll(valuesToAdd);

		Assert.assertEquals(firedEvents.size(), 0);
		Assert.assertTrue(doc.isDirty());
	}

	public void testAddAllNotificationTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		final List<String> valuesToAdd = new ArrayList<String>();
		valuesToAdd.add("value1");
		valuesToAdd.add("value3");


		trackedList.addAll(valuesToAdd);

		Assert.assertTrue(doc.isDirty());
	}

	public void testAddAllNotificationThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		final List<String> valuesToAdd = new ArrayList<String>();
		valuesToAdd.add("value1");
		valuesToAdd.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedList.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				changed.value = true;
			}
		});

		trackedList.addAll(valuesToAdd);

		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testAddIndexNotificationOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.ADD);
				Assert.assertNull(event.getOldValue());
				Assert.assertEquals(event.getKey().intValue(), 1);
				Assert.assertEquals(event.getValue(), "value3");

				changed.value = true;
			}
		});

		trackedList.add(1, "value3");
		Assert.assertEquals(changed.value, Boolean.TRUE);
		Assert.assertTrue(doc.isDirty());
	}

	public void testAddIndexNotificationTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());
		trackedList.add(1, "value3");
		Assert.assertTrue(doc.isDirty());
	}

	public void testAddIndexNotificationThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedList.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				changed.value = true;
			}
		});

		trackedList.add(1, "value3");
		Assert.assertEquals(changed.value, Boolean.FALSE);
		Assert.assertFalse(doc.isDirty());
	}


	public void testSetNotificationOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.UPDATE);
				Assert.assertEquals(event.getOldValue(), "value2");
				Assert.assertEquals(event.getKey().intValue(), 1);
				Assert.assertEquals(event.getValue(), "value4");

				changed.value = true;
			}
		});

		trackedList.set(1, "value4");
		Assert.assertEquals(changed.value, Boolean.TRUE);
		Assert.assertTrue(doc.isDirty());
	}

	public void testSetNotificationTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedList.set(1, "value4");
		Assert.assertTrue(doc.isDirty());
	}

	public void testSetNotificationThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedList.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				changed.value = true;
			}
		});

		trackedList.set(1, "value4");
		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testRemoveNotificationOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.REMOVE);
				Assert.assertEquals(event.getOldValue(), "value2");
				Assert.assertEquals(event.getKey().intValue(), 1);
				Assert.assertNull(event.getValue());

				changed.value = true;
			}
		});

		trackedList.remove("value2");
		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testRemoveNotificationTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());


		trackedList.remove("value2");
		Assert.assertTrue(doc.isDirty());
	}

	public void testRemoveNotificationThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedList.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				changed.value = true;
			}
		});

		trackedList.remove("value2");
		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testRemoveNotificationFour() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				changed.value = true;
			}
		});

		trackedList.remove("value4");
		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}


	public void testRemoveIndexOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.REMOVE);
				Assert.assertEquals(event.getOldValue(), "value2");
				Assert.assertEquals(event.getKey().intValue(), 1);
				Assert.assertNull(event.getValue());

				changed.value = true;
			}
		});

		trackedList.remove(1);
		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testRemoveIndexTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedList.remove(1);
		Assert.assertTrue(doc.isDirty());
	}


	public void testRemoveIndexThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedList.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				changed.value = true;
			}
		});

		trackedList.remove(1);
		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testClearOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final List<OMultiValueChangeEvent<Integer, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Integer, String>>();
		firedEvents.add(new OMultiValueChangeEvent<Integer, String>(OMultiValueChangeEvent.OChangeType.REMOVE, 2, null, "value3"));
		firedEvents.add(new OMultiValueChangeEvent<Integer, String>(OMultiValueChangeEvent.OChangeType.REMOVE, 1, null, "value2"));
		firedEvents.add(new OMultiValueChangeEvent<Integer, String>(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, "value1"));


		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				if (firedEvents.get(0).equals(event))
					firedEvents.remove(0);
				else
					Assert.fail();
			}
		});


		trackedList.clear();
		Assert.assertEquals(0, firedEvents.size());
		Assert.assertTrue(doc.isDirty());
	}

	public void testClearTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedList.clear();
		Assert.assertTrue(doc.isDirty());
	}

	public void testClearThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedList.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				changed.value = true;
			}
		});

		trackedList.clear();
		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}


	public void testReturnOriginalStateOne() {
		final ODocument doc = new ODocument();

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");
		trackedList.add("value4");
		trackedList.add("value5");

		final List<String> original = new ArrayList<String>(trackedList);
		final List<OMultiValueChangeEvent<Integer, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Integer, String>>();

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				firedEvents.add(event);
			}
		});

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

		Assert.assertEquals(original, trackedList.returnOriginalState(firedEvents));
	}

	public void testReturnOriginalStateTwo() {
		final ODocument doc = new ODocument();

		final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
		trackedList.add("value1");
		trackedList.add("value2");
		trackedList.add("value3");
		trackedList.add("value4");
		trackedList.add("value5");

		final List<String> original = new ArrayList<String>(trackedList);
		final List<OMultiValueChangeEvent<Integer, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Integer, String>>();

		trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
				firedEvents.add(event);
			}
		});

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

		Assert.assertEquals(original, trackedList.returnOriginalState(firedEvents));
	}

	/**
	 * Test that {@link OTrackedList} is serialised correctly.
	 */
	@Test
	public void testSerialization() throws Exception {

		class NotSerializableDocument extends ODocument {
			private void writeObject(ObjectOutputStream oos) throws IOException {
				throw new NotSerializableException();
			}
		}

		final OTrackedList<String> beforeSerialization = new OTrackedList<String>(new NotSerializableDocument());
		beforeSerialization.add("firstVal");
		beforeSerialization.add("secondVal");

		final OMemoryStream memoryStream = new OMemoryStream();
		ObjectOutputStream out = new ObjectOutputStream(memoryStream);
		out.writeObject(beforeSerialization);
		out.close();

		final ObjectInputStream input = new ObjectInputStream(new OMemoryInputStream(memoryStream.copy()));
		@SuppressWarnings("unchecked")
		final List<String> afterSerialization = (List<String>) input.readObject();

		Assert.assertEquals(afterSerialization.size(), beforeSerialization.size(), "List size");
		for (int i = 0; i < afterSerialization.size(); i++) {
			Assert.assertEquals(afterSerialization.get(i), beforeSerialization.get(i));
		}
	}
}
