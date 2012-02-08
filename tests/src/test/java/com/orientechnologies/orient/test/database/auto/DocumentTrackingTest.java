package com.orientechnologies.orient.test.database.auto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;


@Test
public class DocumentTrackingTest {
	private final ODatabaseDocumentTx database;

	@Parameters(value = "url")
	public DocumentTrackingTest(final String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@BeforeClass
	public void beforeClass() {
		if (database.isClosed()) {
			database.open("admin", "admin");
		}

		if (!database.getMetadata().getSchema().existsClass("DocumentTrackingTestClass")) {
			final OClass trackedClass = database.getMetadata().getSchema().createClass("DocumentTrackingTestClass");
			trackedClass.createProperty("embeddedlist", OType.EMBEDDEDLIST);
			trackedClass.createProperty("embeddedmap", OType.EMBEDDEDMAP);
			trackedClass.createProperty("embeddedset", OType.EMBEDDEDSET);
			trackedClass.createProperty("linkset", OType.LINKSET);
			trackedClass.createProperty("linklist", OType.LINKLIST);
			trackedClass.createProperty("linkmap", OType.LINKMAP);

			database.getMetadata().getSchema().save();
		}

		database.close();
	}

	@BeforeMethod
	public void beforeMethod() {
		database.open("admin", "admin");
	}

	@AfterMethod
	public void afterMethod() {
		database.close();
	}

	public void testDocumentEmbeddedListTrackingAfterSave() {
		final ODocument document = new ODocument();

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list, OType.EMBEDDEDLIST);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
	}

	public void testDocumentEmbeddedMapTrackingAfterSave() {
		final ODocument document = new ODocument();

		final Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");


		document.field("embeddedmap", map, OType.EMBEDDEDMAP);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final Map<String, String> trackedMap = document.field("embeddedmap");
		trackedMap.put("key2", "value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "key2", "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
	}

	public void testDocumentEmbeddedSetTrackingAfterSave() {
		final ODocument document = new ODocument();

		final Set<String> set = new HashSet<String>();
		set.add("value1");


		document.field("embeddedset", set, OType.EMBEDDEDSET);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Set<String> trackedSet = document.field("embeddedset");
		trackedSet.add("value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "value2", "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
	}

	public void testDocumentLinkSetTrackingAfterSave() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		final ODocument document = new ODocument();

		final Set<ORID> set = new HashSet<ORID>();
		set.add(docOne.getIdentity());


		document.field("linkset", set, OType.LINKSET);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Set<ORID> trackedSet = document.field("linkset");
		trackedSet.add(docTwo.getIdentity());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
	}

	public void testDocumentLinkListTrackingAfterSave() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		final ODocument document = new ODocument();

		final List<ORID> list = new ArrayList<ORID>();
		list.add(docOne.getIdentity());


		document.field("linklist", list, OType.LINKLIST);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final List<ORID> trackedList = document.field("linklist");
		trackedList.add(docTwo.getIdentity());

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
	}

	public void testDocumentLinkMapTrackingAfterSave() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		final ODocument document = new ODocument();

		final Map<String, ORID> map = new HashMap<String, ORID>();
		map.put("key1", docOne.getIdentity());


		document.field("linkmap", map, OType.LINKMAP);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Map<String, ORID> trackedMap = document.field("linkmap");
		trackedMap.put("key2", docTwo.getIdentity());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
	}

	public void testDocumentEmbeddedListTrackingAfterSaveCacheDisabled() {
		database.getLevel1Cache().clear();
		database.getLevel2Cache().clear();

		database.getLevel1Cache().setEnable(false);
		database.getLevel2Cache().setEnable(false);

		final ODocument document = new ODocument();

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list, OType.EMBEDDEDLIST);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});

		database.getLevel1Cache().setEnable(true);
		database.getLevel2Cache().setEnable(true);

	}

	public void testDocumentEmbeddedMapTrackingAfterSaveCacheDisabled() {
		database.getLevel1Cache().clear();
		database.getLevel2Cache().clear();

		database.getLevel1Cache().setEnable(false);
		database.getLevel2Cache().setEnable(false);


		final ODocument document = new ODocument();

		final Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");


		document.field("embeddedmap", map, OType.EMBEDDEDMAP);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final Map<String, String> trackedMap = document.field("embeddedmap");
		trackedMap.put("key2", "value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "key2", "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});

		database.getLevel1Cache().setEnable(true);
		database.getLevel2Cache().setEnable(true);

	}

	public void testDocumentEmbeddedSetTrackingAfterSaveCacheDisabled() {
		database.getLevel1Cache().clear();
		database.getLevel2Cache().clear();

		database.getLevel1Cache().setEnable(false);
		database.getLevel2Cache().setEnable(false);


		final ODocument document = new ODocument();

		final Set<String> set = new HashSet<String>();
		set.add("value1");


		document.field("embeddedset", set, OType.EMBEDDEDSET);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Set<String> trackedSet = document.field("embeddedset");
		trackedSet.add("value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "value2", "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});

		database.getLevel1Cache().setEnable(true);
		database.getLevel2Cache().setEnable(true);

	}

	public void testDocumentLinkSetTrackingAfterSaveCacheDisabled() {
		database.getLevel1Cache().clear();
		database.getLevel2Cache().clear();

		database.getLevel1Cache().setEnable(false);
		database.getLevel2Cache().setEnable(false);


		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		final ODocument document = new ODocument();

		final Set<ORID> set = new HashSet<ORID>();
		set.add(docOne.getIdentity());


		document.field("linkset", set, OType.LINKSET);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Set<ORID> trackedSet = document.field("linkset");
		trackedSet.add(docTwo.getIdentity());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		database.getLevel1Cache().setEnable(true);
		database.getLevel2Cache().setEnable(true);
	}

	public void testDocumentLinkListTrackingAfterSaveCacheDisabled() {
		database.getLevel1Cache().clear();
		database.getLevel2Cache().clear();

		database.getLevel1Cache().setEnable(false);
		database.getLevel2Cache().setEnable(false);


		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		final ODocument document = new ODocument();

		final List<ORID> list = new ArrayList<ORID>();
		list.add(docOne.getIdentity());


		document.field("linklist", list, OType.LINKLIST);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final List<ORID> trackedList = document.field("linklist");
		trackedList.add(docTwo.getIdentity());

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		database.getLevel1Cache().setEnable(true);
		database.getLevel2Cache().setEnable(true);

	}

	public void testDocumentLinkMapTrackingAfterSaveCacheDisabled() {
		database.getLevel1Cache().clear();
		database.getLevel2Cache().clear();

		database.getLevel1Cache().setEnable(false);
		database.getLevel2Cache().setEnable(false);


		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		final ODocument document = new ODocument();

		final Map<String, ORID> map = new HashMap<String, ORID>();
		map.put("key1", docOne.getIdentity());


		document.field("linkmap", map, OType.LINKMAP);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Map<String, ORID> trackedMap = document.field("linkmap");
		trackedMap.put("key2", docTwo.getIdentity());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		database.getLevel1Cache().setEnable(true);
		database.getLevel2Cache().setEnable(true);
	}


	public void testDocumentEmbeddedListTrackingAfterSaveWitClass() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
	}

	public void testDocumentEmbeddedMapTrackingAfterSaveWithClass() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");


		document.field("embeddedmap", map);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final Map<String, String> trackedMap = document.field("embeddedmap");
		trackedMap.put("key2", "value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "key2", "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
	}

	public void testDocumentEmbeddedSetTrackingAfterSaveWithClass() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final Set<String> set = new HashSet<String>();
		set.add("value1");


		document.field("embeddedset", set);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Set<String> trackedSet = document.field("embeddedset");
		trackedSet.add("value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "value2", "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
	}

	public void testDocumentLinkSetTrackingAfterSaveWithClass() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final Set<ORID> set = new HashSet<ORID>();
		set.add(docOne.getIdentity());


		document.field("linkset", set);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Set<ORID> trackedSet = document.field("linkset");
		trackedSet.add(docTwo.getIdentity());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkset");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
	}

	public void testDocumentLinkListTrackingAfterSaveWithClass() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<ORID> list = new ArrayList<ORID>();
		list.add(docOne.getIdentity());


		document.field("linklist", list);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final List<ORID> trackedList = document.field("linklist");
		trackedList.add(docTwo.getIdentity());

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linklist");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
	}

	public void testDocumentLinkMapTrackingAfterSaveWithClass() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final Map<String, ORID> map = new HashMap<String, ORID>();
		map.put("key1", docOne.getIdentity());


		document.field("linkmap", map);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Map<String, ORID> trackedMap = document.field("linkmap");
		trackedMap.put("key2", docTwo.getIdentity());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("linkmap");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
	}

	public void testDocumentEmbeddedListTrackingAfterConversion() {
		final ODocument document = new ODocument();

		final Set<String> set = new HashSet<String>();
		set.add("value1");

		document.field("embeddedlist", set);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist", OType.EMBEDDEDLIST);
		trackedList.add("value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
	}

	public void testDocumentEmbeddedSetTrackingAfterConversion() {
		final ODocument document = new ODocument();

		final List<String> list = new ArrayList<String>();
		list.add("value1");


		document.field("embeddedset", list);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Set<String> trackedSet = document.field("embeddedset", OType.EMBEDDEDSET);
		trackedSet.add("value2");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
		Assert.assertNotNull(timeLine);

		Assert.assertNotNull(timeLine.getMultiValueChangeEvents());

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "value2", "value2"));

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
	}

	public void testDocumentEmbeddedListTrackingAfterReplace() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		final List<String> newTrackedList = new OTrackedList<String>(document);
		document.field("embeddedlist", newTrackedList);
		newTrackedList.add("value3");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
	}

	public void testDocumentEmbeddedMapTrackingAfterReplace() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");


		document.field("embeddedmap", map);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final Map<String, String> trackedMap = document.field("embeddedmap");
		trackedMap.put("key2", "value2");

		final Map<Object, String> newTrackedMap = new OTrackedMap<String>(document);
		document.field("embeddedmap", newTrackedMap);
		newTrackedMap.put("key3", "value3");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedmap");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedmap"});
	}

	public void testDocumentEmbeddedSetTrackingAfterReplace() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final Set<String> set = new HashSet<String>();
		set.add("value1");


		document.field("embeddedset", set);
		document.field("val", 1);
		document.save();

		Assert.assertFalse(document.isDirty());
		Assert.assertEquals(document.getDirtyFields(), new String[]{});

		final Set<String> trackedSet = document.field("embeddedset");
		trackedSet.add("value2");

		final Set<String> newTrackedSet = new OTrackedSet<String>(document);
		document.field("embeddedset", newTrackedSet);
		newTrackedSet.add("value3");

		Assert.assertTrue(document.isDirty());

		final OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedset");
		Assert.assertNull(timeLine);

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedset"});
	}

	public void testRemoveField() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		document.removeField("embeddedlist");

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
		Assert.assertTrue(document.isDirty());
		Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
	}

	public void testTrackingChangesSwitchedOff() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		document.setTrackingChanges(false);

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertTrue(document.isDirty());
		Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
	}

	public void testTrackingChangesSwitchedOn() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		document.setTrackingChanges(false);
		document.setTrackingChanges(true);

		trackedList.add("value3");

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
		Assert.assertTrue(document.isDirty());
		Assert.assertNotNull(document.getCollectionTimeLine("embeddedlist"));

		final List<OMultiValueChangeEvent> firedEvents = new ArrayList<OMultiValueChangeEvent>();
		firedEvents.add(new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 2, "value3"));

		OMultiValueChangeTimeLine timeLine = document.getCollectionTimeLine("embeddedlist");

		Assert.assertEquals(timeLine.getMultiValueChangeEvents(), firedEvents);

	}

	public void testReset() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		document.reset();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertTrue(document.isDirty());
		Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
	}

	public void testClear() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		document.clear();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertTrue(document.isDirty());
		Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
	}

	public void testUnload() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		document.unload();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());
		Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
	}

	public void testUnsetDirty() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		document.unsetDirty();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());
		Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
	}

	public void testReload() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.field("val", 1);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		document.reload();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());
		Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
	}

	public void testRemoveFieldUsingIterator() {
		final ODocument document = new ODocument("DocumentTrackingTestClass");

		final List<String> list = new ArrayList<String>();
		list.add("value1");

		document.field("embeddedlist", list);
		document.save();

		Assert.assertEquals(document.getDirtyFields(), new String[]{});
		Assert.assertFalse(document.isDirty());

		final List<String> trackedList = document.field("embeddedlist");
		trackedList.add("value2");

		final Iterator fieldIterator = document.iterator();
		fieldIterator.next();
		fieldIterator.remove();

		Assert.assertEquals(document.getDirtyFields(), new String[]{"embeddedlist"});
		Assert.assertTrue(document.isDirty());
		Assert.assertNull(document.getCollectionTimeLine("embeddedlist"));
	}

}
