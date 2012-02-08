package com.orientechnologies.orient.core.db.record;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.orientechnologies.common.types.ORef;
import com.orientechnologies.orient.core.record.impl.ODocument;


@Test
public class TrackedSetTest {
	public void testAddOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.ADD);
				Assert.assertNull(event.getOldValue());
				Assert.assertEquals(event.getKey(), "value1");
				Assert.assertEquals(event.getValue(), "value1");

				changed.value = true;
			}
		});

		trackedSet.add("value1");
		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testAddTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		trackedSet.add("value1");
		Assert.assertTrue(doc.isDirty());
	}

	public void testAddThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedSet.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

		trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
				changed.value = true;
			}
		});

		trackedSet.add("value1");

		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testAddFour() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);

		trackedSet.add("value1");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
				changed.value = true;
			}
		});

		trackedSet.add("value1");
		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}


	public void testRemoveNotificationOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		trackedSet.add("value1");
		trackedSet.add("value2");
		trackedSet.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
				Assert.assertEquals(event.getChangeType(), OMultiValueChangeEvent.OChangeType.REMOVE);
				Assert.assertEquals(event.getOldValue(), "value2");
				Assert.assertEquals(event.getKey(), "value2");
				Assert.assertNull(event.getValue());

				changed.value = true;
			}
		});

		trackedSet.remove("value2");
		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testRemoveNotificationTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		trackedSet.add("value1");
		trackedSet.add("value2");
		trackedSet.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedSet.remove("value2");
		Assert.assertTrue(doc.isDirty());
	}

	public void testRemoveNotificationThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		trackedSet.add("value1");
		trackedSet.add("value2");
		trackedSet.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedSet.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
				changed.value = true;
			}
		});

		trackedSet.remove("value2");
		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testRemoveNotificationFour() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		trackedSet.add("value1");
		trackedSet.add("value2");
		trackedSet.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
				changed.value = true;
			}
		});

		trackedSet.remove("value5");
		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testClearOne() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		trackedSet.add("value1");
		trackedSet.add("value2");
		trackedSet.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final Set<OMultiValueChangeEvent<String, String>> firedEvents = new HashSet<OMultiValueChangeEvent<String, String>>();
		firedEvents.add(new OMultiValueChangeEvent<String, String>(OMultiValueChangeEvent.OChangeType.REMOVE, "value1", null, "value1"));
		firedEvents.add(new OMultiValueChangeEvent<String, String>(OMultiValueChangeEvent.OChangeType.REMOVE, "value2", null, "value2"));
		firedEvents.add(new OMultiValueChangeEvent<String, String>(OMultiValueChangeEvent.OChangeType.REMOVE, "value3", null, "value3"));

		final ORef<Boolean> changed = new ORef<Boolean>(false);

		trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
				if (!firedEvents.remove(event))
					Assert.fail();

				changed.value = true;
			}
		});

		trackedSet.clear();

		Assert.assertEquals(firedEvents.size(), 0);
		Assert.assertTrue(changed.value);
		Assert.assertTrue(doc.isDirty());
	}

	public void testClearTwo() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		trackedSet.add("value1");
		trackedSet.add("value2");
		trackedSet.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final ORef<Boolean> changed = new ORef<Boolean>(false);
		trackedSet.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
		trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
				changed.value = true;
			}
		});

		trackedSet.clear();

		Assert.assertFalse(changed.value);
		Assert.assertFalse(doc.isDirty());
	}

	public void testClearThree() {
		final ODocument doc = new ODocument();
		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		trackedSet.add("value1");
		trackedSet.add("value2");
		trackedSet.add("value3");

		doc.unsetDirty();
		Assert.assertFalse(doc.isDirty());

		trackedSet.clear();

		Assert.assertTrue(doc.isDirty());
	}


	public void testReturnOriginalState() {
		final ODocument doc = new ODocument();

		final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
		trackedSet.add("value1");
		trackedSet.add("value2");
		trackedSet.add("value3");
		trackedSet.add("value4");
		trackedSet.add("value5");

		final Set<String> original = new HashSet<String>(trackedSet);
		final List<OMultiValueChangeEvent<String, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<String, String>>();

		trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
			public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
				firedEvents.add(event);
			}
		});

		trackedSet.add("value6");
		trackedSet.remove("value2");
		trackedSet.remove("value5");
		trackedSet.add("value7");
		trackedSet.add("value8");
		trackedSet.remove("value7");
		trackedSet.add("value9");
		trackedSet.add("value10");


		Assert.assertEquals(original, trackedSet.returnOriginalState(firedEvents));
	}

}
