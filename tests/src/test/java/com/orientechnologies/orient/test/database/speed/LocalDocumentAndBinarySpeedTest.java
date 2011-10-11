package com.orientechnologies.orient.test.database.speed;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.index.OIndex;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

public class LocalDocumentAndBinarySpeedTest {

	private static final String	DEFAULT_DB_URL			= "local:database/binarytest";
	private static final String	DEFAULT_DB_USER			= "admin";
	private static final String	DEFAULT_DB_PASSWORD	= "admin";
	private static final int		size								= 64000;
	private static final int		count								= 10000;
	private static final int		load								= 10000;
	private OIndex<?> index;
	private ODatabaseDocumentTx	database;

	@BeforeClass
	public void setUpClass() {
		database = new ODatabaseDocumentTx(DEFAULT_DB_URL);
		if (database.exists()) {
			database.delete();
			database.create();
		} else {
			database.create();
		}
		database.close();
	}

	@AfterClass
	public void tearDownClass() {

	}

	@Test
	public void saveLotOfMixedData() {
		database.open(DEFAULT_DB_USER, DEFAULT_DB_PASSWORD);
		OClass chunk = database.getMetadata().getSchema().createClass("Chunk");
		index = chunk.createProperty("hash", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
		chunk.createProperty("binary", OType.LINK);

		try {
			byte[] data = new byte[size];

			for (int i = 0; i < size; i++) {
				data[i] = (byte) (i % 255);
			}

			ODocument doc = new ODocument(database, "Chunk");

			for (int i = 0; i < count; i++) {
				doc.reset();
				doc.setClassName("Chunk");
				doc.field("hash", "key" + Integer.toString(i));
				doc.field("binary", new ORecordBytes(database, data));
				doc.save();

				ORID rid = doc.getIdentity();
				if (i % 100 == 0)
					System.out.println("ORID=" + rid);
			}
		} finally {
			database.close();
		}
	}

	@Test(dependsOnMethods = "saveLotOfMixedData")
	public void loadRandomMixed() {
		database.open(DEFAULT_DB_USER, DEFAULT_DB_PASSWORD);

		index = database.getMetadata().getSchema().getClass("Chunk").getProperty("hash").getIndexes().iterator().next();
		Assert.assertNotNull(index);

		Set<Integer> alreadyLoaded = new HashSet<Integer>();

		try {
			for (int i = 0; i < load; i++) {
				int rand = (int) (Math.random() * count);

				if (!alreadyLoaded.contains(rand))
					alreadyLoaded.add(rand);
				else
					System.out.println("already loaded");

				OIdentifiable result = (OIdentifiable) index.get("key" + Integer.toString(rand));
				Assert.assertNotNull(result);

				ODocument doc = (ODocument) result.getRecord();
				System.out.println("loaded " + i + "(" + rand + "), binary record: " + doc.field("binary", ORID.class));
				ORecordBytes record = doc.field("binary");
				Assert.assertNotNull(record);
				if (record != null) {
					byte[] data = record.toStream();
					Assert.assertTrue(data.length == size);
				}

				if (i % 100 == 0)
					System.out.println("loaded " + i);
			}
		} finally {
			database.close();
		}
	}
}