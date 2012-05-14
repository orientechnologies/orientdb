/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

@Test(groups = "db")
public class DbCheckTest implements OCommandOutputListener {

	private String	url;

	@Parameters(value = { "url" })
	public DbCheckTest(String iURL) {
		url = iURL;
	}

	@Test
	public void checkDatabaseIntegrity() throws IOException {
		ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
		database.open("admin", "admin");

		boolean result = ((OStorageLocal) database.getStorage()).check(false, this);
		Assert.assertTrue(result);

		database.close();
	}

	@Test(enabled = false)
	public void onMessage(final String iText) {
		System.out.print(iText);
		System.out.flush();
	}
}
