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

import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;

@Test(groups = "dictionary")
public class DictionaryTest {
	private ODatabaseFlat	database;
	private ORecordFlat		record;

	@Parameters(value = "url")
	public DictionaryTest(String iURL) {
		database = new ODatabaseFlat(iURL);
		record = database.newInstance();
	}

	public void testDictionaryCreate() throws IOException {
		database.open("admin", "admin");

		database.getDictionary().put("key1", record.value("Dictionary test!"));

		database.close();
	}

	@Test(dependsOnMethods = "testDictionaryCreate")
	public void testDictionaryLookup() throws IOException {
		database.open("admin", "admin");

		Assert.assertNotNull(database.getDictionary().get("key1"));
		Assert.assertTrue(((ORecordFlat) database.getDictionary().get("key1")).value().equals("Dictionary test!"));

		database.close();
	}

	@Test(dependsOnMethods = "testDictionaryLookup")
	public void testDictionaryUpdate() throws IOException {
		database.open("admin", "admin");

		final long originalSize = database.getDictionary().size();

		database.getDictionary().put("key1", database.newInstance().value("Text changed"));

		database.close();
		database.open("admin", "admin");

		Assert.assertEquals(((ORecordFlat) database.getDictionary().get("key1")).value(), "Text changed");
		Assert.assertEquals(database.getDictionary().size(), originalSize);

		database.close();
	}

	@Test(dependsOnMethods = "testDictionaryUpdate")
	public void testDictionaryDelete() throws IOException {
		database.open("admin", "admin");

		final long originalSize = database.getDictionary().size();
		Assert.assertNotNull(database.getDictionary().remove("key1"));

		database.close();
		database.open("admin", "admin");

		Assert.assertEquals(database.getDictionary().size(), originalSize - 1);

		database.close();
	}

	@Test(dependsOnMethods = "testDictionaryDelete")
	public void testDictionaryMassiveCreate() throws IOException {
		database.open("admin", "admin");

		final long originalSize = database.getDictionary().size();

		// ASSURE TO STORE THE PAGE-SIZE + 3 FORCING THE CREATION OF LEFT AND RIGHT
		final int total = 1000;

		for (int i = total; i > 0; --i) {
			database.getDictionary().put("key-" + (originalSize + i), database.newInstance().value("test-dictionary-" + i));
		}

		for (int i = total; i > 0; --i) {
			record = database.getDictionary().get("key-" + (originalSize + i));
			record.toString().equals("test-dictionary-" + i);
		}

		Assert.assertEquals(database.getDictionary().size(), originalSize + total);

		database.close();
	}

	@Test(dependsOnMethods = "testDictionaryMassiveCreate")
	public void testDictionaryInTx() throws IOException {
		database.open("admin", "admin");

		database.begin();
		database.getDictionary().put("tx-key", database.newInstance().value("tx-test-dictionary"));
		database.commit();

		Assert.assertNotNull(database.getDictionary().get("tx-key"));

		database.close();
	}
}
