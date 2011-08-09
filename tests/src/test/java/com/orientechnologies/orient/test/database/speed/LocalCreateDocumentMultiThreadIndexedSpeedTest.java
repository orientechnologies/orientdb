/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.speed;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMultiThreadTest;
import com.orientechnologies.orient.test.database.base.OrientThreadTest;

@Test(enabled = false)
public class LocalCreateDocumentMultiThreadIndexedSpeedTest extends OrientMultiThreadTest {
	private ODatabaseDocument	database;
	private long							foundObjects;

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
		System.setProperty("url", "remote:localhost/demo");
		LocalCreateDocumentMultiThreadIndexedSpeedTest test = new LocalCreateDocumentMultiThreadIndexedSpeedTest();
		test.data.go(test);
	}

	public LocalCreateDocumentMultiThreadIndexedSpeedTest() {
		super(1000000, 5, CreateObjectsThread.class);

		OProfiler.getInstance().startRecording();
	}

	@Override
	public void init() {
		database = new ODatabaseDocumentTx(System.getProperty("url"));
		database.setProperty("minPool", 2);
		database.setProperty("maxPool", 3);

		if (database.getURL().startsWith("remote:"))
			database.open("admin", "admin");
		else {
			if (database.exists())
				database.delete();

			database.create();
		}

		foundObjects = 0;// database.countClusterElements("Account");

		System.out.println("\nTotal objects in Animal cluster before the test: " + foundObjects);
	}

	@Test(enabled = false)
	public static class CreateObjectsThread extends OrientThreadTest {
		private ODatabaseDocument	database;
		private ODocument					record;
		private Date							date	= new Date();

		@Override
		public void init() {
			database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");
			record = database.newInstance();
			database.declareIntent(new OIntentMassiveInsert());
			database.begin(TXTYPE.NOTX);

			synchronized (LocalCreateDocumentMultiThreadIndexedSpeedTest.class) {
				database.command(new OCommandSQL("truncate class account")).execute();

				OClass c = database.getMetadata().getSchema().getClass("Account");
				if (c == null)
					c = database.getMetadata().getSchema().createClass("Account");

				OProperty p = database.getMetadata().getSchema().getClass("Account").getProperty("id");
				if (p == null)
					p = database.getMetadata().getSchema().getClass("Account").createProperty("id", OType.INTEGER);

				if (!p.isIndexed())
					p.createIndex(INDEX_TYPE.NOTUNIQUE);
			}
		}

		public void cycle() {
			record.reset();

			record.setClassName("Account");
			record.field("id", data.getCyclesDone());
			record.field("name", "Luca");
			record.field("surname", "Garulli");
			record.field("birthDate", date);
			record.field("salary", 3000f + data.getCyclesDone());

			record.save();

			if (data.getCyclesDone() == data.getCycles() - 1)
				database.commit();
		}

		@Override
		public void deinit() throws Exception {
			if (database != null)
				database.close();
			super.deinit();
		}
	}

	@Override
	public void deinit() {
		long total = database.countClusterElements("Account");

		System.out.println("\nTotal objects in Account cluster after the test: " + total);
		System.out.println("Created " + (total - foundObjects));
		Assert.assertEquals(total - foundObjects, threadCycles);

		if (database != null)
			database.close();

		System.out.println(OProfiler.getInstance().dump());

	}
}
