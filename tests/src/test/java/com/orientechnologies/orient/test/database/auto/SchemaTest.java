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
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.graph.TestUtils;

@Test(groups = "schema")
public class SchemaTest {
	private ODatabaseFlat	database;
	private String				url;

	@Parameters(value = "url")
	public SchemaTest(String iURL) {
		url = iURL;
	}

	public void createSchema() throws IOException {
		database = new ODatabaseFlat(url);
		if (TestUtils.existsDatabase(database))
			database.open("admin", "admin");
		else
			database.create();

		if (database.getMetadata().getSchema().existsClass("Account"))
			return;

		database.addCluster("csv", OStorage.CLUSTER_TYPE.PHYSICAL);
		database.addCluster("flat", OStorage.CLUSTER_TYPE.PHYSICAL);
		database.addCluster("binary", OStorage.CLUSTER_TYPE.PHYSICAL);

		OClass account = database.getMetadata().getSchema()
				.createClass("Account", database.addCluster("account", OStorage.CLUSTER_TYPE.PHYSICAL));
		account.createProperty("id", OType.INTEGER);
		account.createProperty("birthDate", OType.DATE);
		account.createProperty("binary", OType.BINARY);

		database.getMetadata().getSchema().createClass("Company", account);

		OClass profile = database.getMetadata().getSchema()
				.createClass("Profile", database.addCluster("profile", OStorage.CLUSTER_TYPE.PHYSICAL));
		profile.createProperty("nick", OType.STRING).setMin("3").setMax("30").createIndex(OClass.INDEX_TYPE.UNIQUE);
		profile.createProperty("name", OType.STRING).setMin("3").setMax("30").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
		profile.createProperty("surname", OType.STRING).setMin("3").setMax("30");
		profile.createProperty("registeredOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
		profile.createProperty("lastAccessOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
		profile.createProperty("photo", OType.TRANSIENT);

		OClass whiz = database.getMetadata().getSchema().createClass("Whiz");
		whiz.createProperty("id", OType.INTEGER);
		whiz.createProperty("account", OType.LINK, account);
		whiz.createProperty("date", OType.DATE).setMin("2010-01-01");
		whiz.createProperty("text", OType.STRING).setMandatory(true).setMin("1").setMax("140").createIndex(OClass.INDEX_TYPE.FULLTEXT);
		whiz.createProperty("replyTo", OType.LINK, account);

		database.close();
	}

	@Test(dependsOnMethods = "createSchema")
	public void checkSchema() {
		database = new ODatabaseFlat(url);
		database.open("admin", "admin");

		OSchema schema = database.getMetadata().getSchema();

		assert schema != null;
		assert schema.getClass("Profile") != null;
		assert schema.getClass("Profile").getProperty("nick").getType() == OType.STRING;
		assert schema.getClass("Profile").getProperty("name").getType() == OType.STRING;
		assert schema.getClass("Profile").getProperty("surname").getType() == OType.STRING;
		assert schema.getClass("Profile").getProperty("registeredOn").getType() == OType.DATETIME;
		assert schema.getClass("Profile").getProperty("lastAccessOn").getType() == OType.DATETIME;

		assert schema.getClass("Whiz") != null;
		assert schema.getClass("whiz").getProperty("account").getType() == OType.LINK;
		assert schema.getClass("whiz").getProperty("account").getLinkedClass().getName().equalsIgnoreCase("Account");
		assert schema.getClass("WHIZ").getProperty("date").getType() == OType.DATE;
		assert schema.getClass("WHIZ").getProperty("text").getType() == OType.STRING;
		assert schema.getClass("WHIZ").getProperty("text").isMandatory();
		assert schema.getClass("WHIZ").getProperty("text").getMin().equals("1");
		assert schema.getClass("WHIZ").getProperty("text").getMax().equals("140");
		assert schema.getClass("whiz").getProperty("replyTo").getType() == OType.LINK;
		assert schema.getClass("Whiz").getProperty("replyTo").getLinkedClass().getName().equalsIgnoreCase("Account");

		database.close();
	}

	@Test(dependsOnMethods = "checkSchema")
	public void checkSchemaApi() {
		database = new ODatabaseFlat(url);
		database.open("admin", "admin");

		OSchema schema = database.getMetadata().getSchema();

		try {
			Assert.assertNull(schema.getClass("Animal33"));
		} catch (OSchemaException e) {
		}

		database.close();
	}

	@Test(dependsOnMethods = "checkSchemaApi")
	public void checkClusters() {
		database = new ODatabaseFlat(url);
		database.open("admin", "admin");

		for (OClass cls : database.getMetadata().getSchema().getClasses()) {
			assert database.getClusterNameById(cls.getDefaultClusterId()) != null;
		}

		database.close();
	}

	@Test(dependsOnMethods = "createSchema")
	public void checkDatabaseSize() {
		database = new ODatabaseFlat(url);
		database.open("admin", "admin");

		Assert.assertTrue(database.getStorage().getSize() > 0);

		database.close();
	}

	@Test(dependsOnMethods = "createSchema")
	public void checkTotalRecords() {
		database = new ODatabaseFlat(url);
		database.open("admin", "admin");

		Assert.assertTrue(database.getStorage().countRecords() > 0);

		database.close();
	}

	@Test(expectedExceptions = OValidationException.class)
	public void checkErrorOnUserNoPasswd() {
		database = new ODatabaseFlat(url);
		database.open("admin", "admin");

		database.getMetadata().getSecurity().createUser("error", null, null);

		database.close();
	}

	@Test
	public void testMultiThreadSchemaCreation() throws InterruptedException {
		database = new ODatabaseFlat(url);
		database.open("admin", "admin");

		Thread thread = new Thread(new Runnable() {

			public void run() {
				ODocument doc = new ODocument(database, "NewClass");
				database.save(doc);

				doc.delete();
				database.getMetadata().getSchema().dropClass("NewClass");

				database.close();
			}
		});

		thread.start();
		thread.join();
	}
}
