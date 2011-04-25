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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

@Test(groups = "db")
public class DbDeleteTest {
	private String	testPath;

	@Parameters(value = { "url", "testPath" })
	public DbDeleteTest(String iURL, String iTestPath) {
		testPath = iTestPath;
		OProfiler.getInstance().startRecording();
	}

	public void testDbDelete() throws IOException {
		new ODatabaseDocumentTx("local:" + testPath + "/" + DbImportExportTest.NEW_DB_URL).delete();

		Assert.assertFalse(new File(testPath + "/" + DbImportExportTest.NEW_DB_PATH).exists());
	}
}
