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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test(groups = "security")
public class SecurityTest {
	private ODatabaseDocumentTx	database;

	@Parameters(value = "url")
	public SecurityTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void testWrongPassword() throws IOException {
		try {
			database.open("reader", "swdsds");
		} catch (ODatabaseException e) {
			Assert
					.assertTrue(e.getCause() instanceof OSecurityAccessException
							|| e.getCause().toString().indexOf("com.orientechnologies.orient.core.exception.OSecurityAccessException") > -1);
		}
	}

	@Test
	public void testSecurityAccessWriter() throws IOException {
		database.open("writer", "writer");

		try {
			new ODocument(database, "Profile").save("internal");
			Assert.assertTrue(false);
		} catch (ODatabaseException e) {
			Assert.assertTrue(e.getCause() instanceof OSecurityAccessException);
		} finally {
			database.close();
		}
	}

	@Test
	public void testSecurityAccessReader() throws IOException {
		database.open("reader", "reader");

		try {
			new ODocument(database, "Profile").save();
		} catch (ODatabaseException e) {
			Assert.assertTrue(e.getCause() instanceof OSecurityAccessException);
		} finally {
			database.close();
		}
	}
}
