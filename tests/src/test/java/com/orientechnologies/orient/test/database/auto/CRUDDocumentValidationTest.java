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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

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

		account = new ODocument("Account");
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

	@Test(dependsOnMethods = "validationEmbeddedType", expectedExceptions = OValidationException.class)
	public void validationStrictClass() throws ParseException {
		ODocument doc = new ODocument("StrictTest");
		doc.field("id", 122112);
		doc.field("antani", "122112");
		doc.save();
	}

	@Test(dependsOnMethods = "validationStrictClass")
	public void closeDb() {
		database.close();
	}

	@Test(dependsOnMethods = "closeDb")
	public void createSchemaForMandatoryNullableTest() throws ParseException {
		database.open("admin", "admin");
		database.command(new OCommandSQL("CREATE CLASS MyTestClass")).execute();
		database.command(new OCommandSQL("CREATE PROPERTY MyTestClass.keyField STRING")).execute();
		database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.keyField MANDATORY true")).execute();
		database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.keyField NOTNULL true")).execute();
		database.command(new OCommandSQL("CREATE PROPERTY MyTestClass.dateTimeField DATETIME")).execute();
		database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.dateTimeField MANDATORY true")).execute();
		database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.dateTimeField NOTNULL false")).execute();
		database.command(new OCommandSQL("CREATE PROPERTY MyTestClass.stringField STRING")).execute();
		database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.stringField MANDATORY true")).execute();
		database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.stringField NOTNULL false")).execute();
		database.command(new OCommandSQL("INSERT INTO MyTestClass (keyField,dateTimeField,stringField) VALUES (\"K1\",null,null)"))
				.execute();
		database.reload();
		database.getStorage().reload();
		database.getMetadata().reload();
		database.close();
		database.open("admin", "admin");
		OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM MyTestClass WHERE keyField = ?");
		List<ODocument> result = database.query(query, "K1");
		Assert.assertEquals(1, result.size());
		ODocument doc = result.get(0);
		Assert.assertTrue(doc.containsField("keyField"));
		Assert.assertTrue(doc.containsField("dateTimeField"));
		Assert.assertTrue(doc.containsField("stringField"));
		database.close();
	}

	@Test(dependsOnMethods = "createSchemaForMandatoryNullableTest")
	public void testUpdateDocDefined() {
		database.open("admin", "admin");
		OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM MyTestClass WHERE keyField = ?");
		List<ODocument> result = database.query(query, "K1");
		Assert.assertEquals(1, result.size());
		ODocument doc = result.get(0);
		doc.field("keyField", "K1N");
		doc.save();
		database.close();
	}

	@Test(dependsOnMethods = "testUpdateDocDefined")
	public void validationMandatoryNullableCloseDb() throws ParseException {
		database.open("admin", "admin");
		ODocument doc = new ODocument("MyTestClass");
		doc.field("keyField", "K2");
		doc.field("dateTimeField", (Date) null);
		doc.field("stringField", (String) null);
		doc.save();

		database.close();
		database.open("admin", "admin");

		OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM MyTestClass WHERE keyField = ?");
		List<ODocument> result = database.query(query, "K2");
		Assert.assertEquals(1, result.size());
		doc = result.get(0);
		doc.field("keyField", "K2N");
		doc.save();
		database.close();
	}

	@Test(dependsOnMethods = "validationMandatoryNullableCloseDb")
	public void validationMandatoryNullableNoCloseDb() throws ParseException {
		database.open("admin", "admin");
		ODocument doc = new ODocument("MyTestClass");
		doc.field("keyField", "K3");
		doc.field("dateTimeField", (Date) null);
		doc.field("stringField", (String) null);
		doc.save();

		OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM MyTestClass WHERE keyField = ?");
		List<ODocument> result = database.query(query, "K3");
		Assert.assertEquals(1, result.size());
		doc = result.get(0);
		doc.field("keyField", "K3N");
		doc.save();
		database.close();
	}

	@Test(dependsOnMethods = "validationMandatoryNullableNoCloseDb")
	public void dropSchemaForMandatoryNullableTest() throws ParseException {
		database.open("admin", "admin");
		database.command(new OCommandSQL("DROP CLASS MyTestClass")).execute();
		database.close();
	}
}
