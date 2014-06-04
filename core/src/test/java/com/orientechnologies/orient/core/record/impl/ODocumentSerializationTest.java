package com.orientechnologies.orient.core.record.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;

public class ODocumentSerializationTest {

	private ORecordSerializer	serializer;

	public ODocumentSerializationTest(ORecordSerializer serializer) {
		this.serializer = serializer;
	}

	public ODocumentSerializationTest() {
		this(new ORecordSerializerSchemaAware2CSV());
	}

	@Test
	public void testSimpleSerialization() {
		ODocument document = new ODocument();

		document.field("name", "name");
		document.field("age", 20);
		document.field("youngAge", (short) 20);
		document.field("oldAge", (long) 20);
		document.field("heigth", 12.5f);
		document.field("bitHeigth", 12.5d);
		document.field("class", (byte) 'C');
		document.field("character", 'C');
		document.field("alive", true);
		document.field("date", new Date());
		document.field("recordId", new ORecordId(10, new OClusterPositionLong(10)));

		byte[] res = serializer.toStream(document, false);
		ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

		Assert.assertEquals(document.fields(), extr.fields());
		Assert.assertEquals(document.field("name"), extr.field("name"));
		Assert.assertEquals(document.field("age"), extr.field("age"));
		Assert.assertEquals(document.field("youngAge"), extr.field("youngAge"));
		Assert.assertEquals(document.field("oldAge"), extr.field("oldAge"));
		Assert.assertEquals(document.field("heigth"), extr.field("heigth"));
		Assert.assertEquals(document.field("bitHeigth"), extr.field("bitHeigth"));
		Assert.assertEquals(document.field("class"), extr.field("class"));
		// TODO fix char management issue:#2427
		// Assert.assertEquals(document.field("character"), extr.field("character"));
		Assert.assertEquals(document.field("alive"), extr.field("alive"));
		Assert.assertEquals(document.field("date"), extr.field("date"));
		Assert.assertEquals(document.field("recordId"), extr.field("recordId"));

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSimpleLiteralList() {

		ODocument document = new ODocument();
		List<String> strings = new ArrayList<String>();
		strings.add("a");
		strings.add("b");
		strings.add("c");
		document.field("listStrings", strings);

		List<Short> shorts = new ArrayList<Short>();
		shorts.add((short) 1);
		shorts.add((short) 2);
		shorts.add((short) 3);
		document.field("shorts", shorts);

		List<Long> longs = new ArrayList<Long>();
		longs.add((long) 1);
		longs.add((long) 2);
		longs.add((long) 3);
		document.field("longs", longs);

		List<Integer> ints = new ArrayList<Integer>();
		ints.add(1);
		ints.add(2);
		ints.add(3);
		document.field("integers", ints);

		List<Float> floats = new ArrayList<Float>();
		floats.add(1.1f);
		floats.add(2.2f);
		floats.add(3.3f);
		document.field("floats", floats);

		List<Double> doubles = new ArrayList<Double>();
		doubles.add(1.1);
		doubles.add(2.2);
		doubles.add(3.3);
		document.field("doubles", doubles);

		List<Date> dates = new ArrayList<Date>();
		dates.add(new Date());
		dates.add(new Date());
		dates.add(new Date());
		document.field("dates", dates);

		List<Byte> bytes = new ArrayList<Byte>();
		bytes.add((byte) 0);
		bytes.add((byte) 1);
		bytes.add((byte) 3);
		document.field("bytes", bytes);

		// TODO: char not currently supported in orient.
		List<Character> chars = new ArrayList<Character>();
		chars.add('A');
		chars.add('B');
		chars.add('C');
		// document.field("chars", chars);

		List<Boolean> booleans = new ArrayList<Boolean>();
		booleans.add(true);
		booleans.add(false);
		booleans.add(false);
		document.field("booleans", booleans);

		List listMixed = new ArrayList();
		listMixed.add(true);
		listMixed.add(1);
		listMixed.add((long) 5);
		listMixed.add((short) 2);
		listMixed.add(4.0f);
		listMixed.add(7.0D);
		listMixed.add("hello");
		listMixed.add(new Date());
		listMixed.add((byte) 10);
		document.field("listMixed", listMixed);

		byte[] res = serializer.toStream(document, false);
		ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

		Assert.assertEquals(document.fields(), extr.fields());
		Assert.assertEquals(document.field("listStrings"), extr.field("listStrings"));
		Assert.assertEquals(document.field("integers"), extr.field("integers"));
		Assert.assertEquals(document.field("doubles"), extr.field("doubles"));
		Assert.assertEquals(document.field("dates"), extr.field("dates"));
		Assert.assertEquals(document.field("bytes"), extr.field("bytes"));
		Assert.assertEquals(document.field("booleans"), extr.field("booleans"));
		Assert.assertEquals(document.field("listMixed"), extr.field("listMixed"));
	}

	@Test
	public void testSimpleMapStringLiteral() {
		ODocument document = new ODocument();

		Map<String, String> mapString = new HashMap<String, String>();
		mapString.put("key", "value");
		mapString.put("key1", "value1");
		document.field("mapString", mapString);

		Map<String, Integer> mapInt = new HashMap<String, Integer>();
		mapInt.put("key", 2);
		mapInt.put("key1", 3);
		document.field("mapInt", mapInt);

		Map<String, Long> mapLong = new HashMap<String, Long>();
		mapLong.put("key", 2L);
		mapLong.put("key1", 3L);
		document.field("mapLong", mapLong);

		Map<String, Short> shortMap = new HashMap<String, Short>();
		shortMap.put("key", (short) 2);
		shortMap.put("key1", (short) 3);
		document.field("shortMap", shortMap);

		Map<String, Date> dateMap = new HashMap<String, Date>();
		dateMap.put("key", new Date());
		dateMap.put("key1", new Date());
		document.field("dateMap", dateMap);

		Map<String, Float> floatMap = new HashMap<String, Float>();
		floatMap.put("key", 10f);
		floatMap.put("key1", 11f);
		document.field("floatMap", floatMap);

		Map<String, Double> doubleMap = new HashMap<String, Double>();
		doubleMap.put("key", 10d);
		doubleMap.put("key1", 11d);
		document.field("doubleMap", doubleMap);

		Map<String, Byte> bytesMap = new HashMap<String, Byte>();
		bytesMap.put("key", (byte) 10);
		bytesMap.put("key1", (byte) 11);
		document.field("bytesMap", bytesMap);

		byte[] res = serializer.toStream(document, false);
		ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
		Assert.assertEquals(document.fields(), extr.fields());
		Assert.assertEquals(document.field("mapString"), extr.field("mapString"));
		Assert.assertEquals(document.field("mapLong"), extr.field("mapLong"));
		Assert.assertEquals(document.field("shortMap"), extr.field("shortMap"));
		Assert.assertEquals(document.field("dateMap"), extr.field("dateMap"));
		Assert.assertEquals(document.field("doubleMap"), extr.field("doubleMap"));
		Assert.assertEquals(document.field("bytesMap"), extr.field("bytesMap"));
	}

	@Test
	public void testSimpleEmbeddedDoc() {
		ODocument document = new ODocument();
		ODocument embedded = new ODocument();
		embedded.field("name", "test");
		embedded.field("surname", "something");
		document.field("embed", embedded);

		byte[] res = serializer.toStream(document, false);
		ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
		Assert.assertEquals(document.fields(), extr.fields());
		ODocument emb = document.field("embed");
		Assert.assertNotNull(emb);
		Assert.assertEquals(embedded.field("name"), emb.field("name"));
		Assert.assertEquals(embedded.field("surname"), emb.field("surname"));

	}

}
