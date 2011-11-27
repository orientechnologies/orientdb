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

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test(groups = { "crud", "record-document" })
public class CRUDDocumentValidationTest {
	protected static final int	TOT_RECORDS	= 100;
	protected long							startRecordNumber;
	private ODatabaseDocumentTx	database;
	private ODocument						record;
	private ODocument						account;

	@Parameters(value = "url")
	public CRUDDocumentValidationTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void openDb() {
		database.open("admin", "admin");
		record = database.newInstance("Whiz");

		account = new ODocument(database, "Account");
		account.field("id", "1234567890");
	}

	@Test(dependsOnMethods = "openDb", expectedExceptions = OValidationException.class)
	public void validationMandatory() {
		record.clear();
		record.save();
	}

	@Test(dependsOnMethods = "validationMandatory", expectedExceptions = OValidationException.class)
	public void validationMinString() {
		record.clear();
		record.field("account", account);
		record.field("id", 23723);
		record.field("text", "");
		record.save();
	}

	@Test(dependsOnMethods = "validationMinString", expectedExceptions = OValidationException.class, expectedExceptionsMessageRegExp = ".*more.*than.*")
	public void validationMaxString() {
		record.clear();
		record.field("account", account);
		record.field("id", 23723);
		record
				.field(
						"text",
						"clfdkkjsd hfsdkjhf fjdkghjkfdhgjdfh gfdgjfdkhgfd skdjaksdjf skdjf sdkjfsd jfkldjfkjsdf kljdk fsdjf kldjgjdhjg khfdjgk hfjdg hjdfhgjkfhdgj kfhdjghrjg");
		record.save();
	}

	@Test(dependsOnMethods = "validationMaxString", expectedExceptions = OValidationException.class, expectedExceptionsMessageRegExp = ".*precedes.*")
	public void validationMinDate() throws ParseException {
		record.clear();
		record.field("account", account);
		record.field("date", new SimpleDateFormat("dd/MM/yyyy").parse("01/33/1976"));
		record.field("text", "test");
		record.save();
	}

	@Test(dependsOnMethods = "validationMinDate", expectedExceptions = OValidationException.class)
	public void validationEmbeddedType() throws ParseException {
		record.clear();
		record.field("account", database.getUser());
		record.save();
	}

	@Test(dependsOnMethods = "validationEmbeddedType")
	public void closeDb() {
		database.close();
	}
}
