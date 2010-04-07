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

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.record.impl.ORecordDocument;

@Test(groups = { "crud", "record-vobject" }, sequential = true)
public class CRUDDocumentValidationTest {
	protected static final int	TOT_RECORDS	= 1000;
	protected long							startRecordNumber;
	private ODatabaseDocumentTx	database;
	private ORecordDocument			record;

	@Parameters(value = "url")
	public CRUDDocumentValidationTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void openDb() {
		database.open("admin", "admin");
		record = database.newInstance("Animal");
	}

	@Test(dependsOnMethods = "openDb", expectedExceptions = OValidationException.class)
	public void validationMandatory() {
		record.reset();
		record.field("type", "ciao!");
		record.save("Animal");
	}

	@Test(dependsOnMethods = "validationMandatory", expectedExceptions = OValidationException.class)
	public void validationMinString() {
		record.reset();
		record.field("id", 23723);
		record.field("name", "c");
		record.save("Animal");
	}

	@Test(dependsOnMethods = "validationMinString", expectedExceptions = OValidationException.class, expectedExceptionsMessageRegExp = ".*more.*than.*")
	public void validationMaxString() {
		record.reset();
		record.field("id", 23723);
		record.field("name", "clfdkkjsd hfsdkjhf fjdkghjkfdhgjdfh gfdgjfdkhgfd");
		record.save("Animal");
	}

	@Test(dependsOnMethods = "validationMaxString", expectedExceptions = OValidationException.class, expectedExceptionsMessageRegExp = ".*minor.*")
	public void validationMinFloat() {
		record.reset();
		record.field("id", 23723);
		record.field("name", "Lucas");
		record.field("price", -38923);
		record.save("Animal");
	}

	@Test(dependsOnMethods = "validationMinFloat")
	public void closeDb() {
		database.close();
	}
}
