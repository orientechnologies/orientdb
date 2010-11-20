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
	private static final String	DB2_PATH	= "tests/target/test-import";
	private static final String	DB2_URL		= "local:tests/target/test-import/test-import";

	@Parameters(value = "url")
	public DbDeleteTest(String iURL) {
		OProfiler.getInstance().startRecording();
	}

	public void testDbDelete() throws IOException {
		new ODatabaseDocumentTx(DB2_URL).delete();

		Assert.assertFalse(new File(DB2_PATH).exists());
	}
}
