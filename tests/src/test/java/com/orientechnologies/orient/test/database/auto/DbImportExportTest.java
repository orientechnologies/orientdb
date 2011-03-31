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
import java.util.ArrayList;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.hook.ORecordHook;

@Test(groups = "db")
public class DbImportExportTest implements OCommandOutputListener {
	public static final String	EXPORT_FILE_PATH	= "target/db.export.gz";
	public static final String	NEW_DB_PATH				= "target/test-import";
	public static final String	NEW_DB_URL				= "target/test-import";

	private String							url;
	private String							testPath;

	@Parameters(value = { "url", "testPath" })
	public DbImportExportTest(String iURL, String iTestPath) {
		url = iURL;
		testPath = iTestPath;
		OProfiler.getInstance().startRecording();
	}

	@Test
	public void testDbExport() throws IOException {
		ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
		database.open("admin", "admin");

		ODatabaseExport export = new ODatabaseExport(database, testPath + "/" + EXPORT_FILE_PATH, this);
		export.exportDatabase();
		export.close();

		database.close();
	}

	@Test(dependsOnMethods = "testDbExport")
	public void testDbImport() throws IOException {
		final File importDir = new File(testPath + "/" + NEW_DB_PATH);
		if (importDir.exists())
			for (File f : importDir.listFiles())
				f.delete();
		else
			importDir.mkdir();

		ODatabaseDocumentTx database = new ODatabaseDocumentTx("local:" + testPath + "/" + NEW_DB_URL);
		database.create();

		ODatabaseImport impor = new ODatabaseImport(database, testPath + "/" + EXPORT_FILE_PATH, this);

		// UNREGISTER ALL THE HOOKS
		for (ORecordHook hook : new ArrayList<ORecordHook>(database.getHooks())) {
			database.unregisterHook(hook);
		}

		impor.importDatabase();
		impor.close();

		database.close();
	}

	@Test(enabled = false)
	public void onMessage(final String iText) {
		System.out.print(iText);
		System.out.flush();
	}
}
