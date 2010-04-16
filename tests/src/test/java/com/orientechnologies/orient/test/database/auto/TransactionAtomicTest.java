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
package com.orientechnologies.orient.test.database.auto;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;

@Test(groups = "dictionary")
public class TransactionAtomicTest {
	private String	url;

	@Parameters(value = "url")
	public TransactionAtomicTest(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());
		url = iURL;
	}

	@Test
	public void testTransactionAtomic() throws IOException {
		ODatabaseFlat db1 = new ODatabaseFlat(url);
		db1.open("admin", "admin");

		ODatabaseFlat db2 = new ODatabaseFlat(url);
		db2.open("admin", "admin");

		ORecordFlat record1 = new ORecordFlat(db1);
		record1.value("This is the first version").save();

		// RE-READ THE RECORD
		record1.load();
		ORecordFlat record2 = db2.load(record1.getIdentity());

		record2.value("This is the second version").save();
		record1.value("This is the third version").save();

		record1 = db1.load(record1.getIdentity());

		Assert.assertTrue(record1.value().equals("This is the third version"));

		db1.close();
		db2.close();
	}
}
