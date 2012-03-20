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

import java.io.File;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;

@Test(groups = "db")
public class DbDeleteTest {
	private String	testPath;
	private String	url;

	@Parameters(value = { "url", "testPath" })
	public DbDeleteTest(String iURL, String iTestPath) {
		testPath = iTestPath;
		url = iURL;
		OProfiler.getInstance().startRecording();
	}

	public void testDbDeleteNoCredential() throws IOException {
		ODatabaseDocument db = new ODatabaseDocumentTx(url);
		try {
			db.drop();
			Assert.fail("Should have thrown ODatabaseException because trying to delete a not opened");
		} catch (ODatabaseException e) {
			Assert.assertTrue(e.getMessage().equals("Database '" + url + "' is closed"));
		} catch (OStorageException e) {
			Assert.assertTrue(e.getMessage().startsWith("Cannot delete the remote storage:"));
		}
	}

	@Test(dependsOnMethods = { "testDbDeleteNoCredential" })
	public void testDbDelete() throws IOException {
		ODatabaseDocument db = new ODatabaseDocumentTx("local:" + testPath + "/" + DbImportExportTest.NEW_DB_URL);
		ODatabaseHelper.dropDatabase(db);

		Assert.assertFalse(new File(testPath + "/" + DbImportExportTest.NEW_DB_PATH).exists());
	}
}
