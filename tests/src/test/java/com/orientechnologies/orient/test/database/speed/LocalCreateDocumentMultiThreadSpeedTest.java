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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMultiThreadTest;
import com.orientechnologies.orient.test.database.base.OrientThreadTest;

@Test(enabled = false)
public class LocalCreateDocumentMultiThreadSpeedTest extends OrientMultiThreadTest {
	private ODatabaseDocument	database;
	private long							foundObjects;

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
		// System.setProperty("url", "memory:test");
		LocalCreateDocumentMultiThreadSpeedTest test = new LocalCreateDocumentMultiThreadSpeedTest();
		test.data.go(test);
	}

	public LocalCreateDocumentMultiThreadSpeedTest() {
		super(1000000, 200, CreateObjectsThread.class);
	}

	@Override
	public void init() {
		database = new ODatabaseDocumentTx(System.getProperty("url"));
		if (database.exists())
			// database.open("admin", "admin");
			// else
			database.delete();
		
		database.create();

		foundObjects = 0;//database.countClusterElements("Account");

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
	}
}
