package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Random;

public class PartialSearchIndexSpeedTest extends SpeedTestMonoThread {
	private ODatabaseDocumentTx database = new ODatabaseDocumentTx("local:partialSearchIndexSpeedTest");

	public PartialSearchIndexSpeedTest() {
		super(1000);
	}

	@Override
	@Test(enabled = false)
	public void cycle() throws Exception {
		int start = (new Random()).nextInt(10);
		final OIndex index = database.getMetadata().getIndexManager().getIndex("testIndex");
		final Collection<ODocument> result = index.getEntriesMajor(new OCompositeKey(start), true, 100);
		Assert.assertEquals(result.size(), 100);
	}

	@Override
	@Test(enabled = false)
	public void init() throws Exception {
		if (!database.exists()) {
			database.create();

			final OSchema schema = database.getMetadata().getSchema();
			final OClass testClass = schema.createClass("IndexTest");
			testClass.createProperty("f1", OType.INTEGER);
			testClass.createProperty("f2", OType.INTEGER);

			testClass.createIndex("testIndex", OClass.INDEX_TYPE.UNIQUE, "f1", "f2");

			schema.save();

			final ODocument document = new ODocument();
			for (int i = 0; i < 10; i++) {
				for (int j = 0; j < 500000; j++) {
					document.reset();

					document.setClassName("IndexTest");
					document.field("f1", i);
					document.field("f2", j);

					document.save();
				}
			}
		} else {
			database.open("admin", "admin");
		}
	}
}