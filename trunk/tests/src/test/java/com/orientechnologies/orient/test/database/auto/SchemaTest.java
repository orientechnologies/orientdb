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

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.record.ODatabaseRecordFlat;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;

@Test(groups = "schema")
public class SchemaTest {
	private ODatabaseRecordFlat	database;
	private String							url;

	@Parameters(value = "url")
	public SchemaTest(String iURL) {
		url = iURL;
	}

	public void createSchema() {
		database = new ODatabaseRecordFlat(url);
		database.open("admin", "admin");

		if (database.getMetadata().getSchema().existsClass("Animal"))
			return;

		database.getStorage().addCluster("csv");
		database.getStorage().addCluster("binary");

		OClass person = database.getMetadata().getSchema().createClass("Person", database.getStorage().addCluster("person"));

		OClass animal = database.getMetadata().getSchema().createClass("Animal", database.getStorage().addCluster("animal"));
		animal.createProperty("id", OType.INTEGER).setMandatory(true);
		animal.createProperty("name", OType.STRING).setMin("3").setMax("30");
		animal.createProperty("type", OType.STRING);
		animal.createProperty("race", OType.STRING);
		animal.createProperty("location", OType.STRING);
		animal.createProperty("price", OType.FLOAT).setMin("0");

		OClass race = database.getMetadata().getSchema().createClass("AnimalRace");
		race.createProperty("name", OType.STRING).setMandatory(true).setMin("3");

		OClass animalType = database.getMetadata().getSchema().createClass("AnimalType");
		animalType.createProperty("name", OType.STRING).setMandatory(true);
		animalType.createProperty("races", OType.EMBEDDEDSET, race);

		OClass orderItem = database.getMetadata().getSchema().createClass("OrderItem");
		orderItem.createProperty("id", OType.INTEGER);
		orderItem.createProperty("animal", OType.LINK, animal);
		orderItem.createProperty("quantity", OType.INTEGER).setMin("1");
		orderItem.createProperty("price", OType.FLOAT).setMin("0");

		OClass order = database.getMetadata().getSchema().createClass("Order");
		order.createProperty("id", OType.INTEGER).setMandatory(true);
		order.createProperty("date", OType.DATE).setMin("2009-01-01 00:00:00");
		order.createProperty("customer", OType.STRING);
		order.createProperty("items", OType.EMBEDDEDLIST, orderItem);
		order.createProperty("totalPrice", OType.FLOAT);

		database.getMetadata().getSchema().save();
		database.close();
	}

	@Test(dependsOnMethods = "createSchema")
	public void checkSchema() {
		database = new ODatabaseRecordFlat(url);
		database.open("admin", "admin");

		OSchema schema = database.getMetadata().getSchema();

		assert schema != null;
		assert schema.getClass("Animal") != null;
		assert schema.getClass("ANIMAL").getProperty("id").getType() == OType.INTEGER;
		assert schema.getClass("animal").getProperty("name").getType() == OType.STRING;
		assert schema.getClass("AnimAL").getProperty("race").getType() == OType.STRING;
		assert schema.getClass("Animal").getProperty("location").getType() == OType.STRING;
		assert schema.getClass("Animal").getProperty("price").getType() == OType.FLOAT;

		assert schema.getClass("AnimalRace") != null;
		assert schema.getClass("Animalrace").getProperty("name").getType() == OType.STRING;

		assert schema.getClass("AnimalType") != null;
		assert schema.getClass("Animaltype").getProperty("name").getType() == OType.STRING;
		assert schema.getClass("Animaltype").getProperty("races").getType() == OType.EMBEDDEDSET;
		assert schema.getClass("Animaltype").getProperty("races").getLinkedClass().getName().equalsIgnoreCase("AnimalRace");

		assert schema.getClass("OrderItem") != null;
		assert schema.getClass("ORDERITEM").getProperty("id").getType() == OType.INTEGER;
		assert schema.getClass("ORDERITEM").getProperty("animal").getType() == OType.LINK;
		assert schema.getClass("ORDERITEM").getProperty("quantity").getType() == OType.INTEGER;
		assert schema.getClass("ORDERITEM").getProperty("price").getType() == OType.FLOAT;

		assert schema.getClass("Order") != null;
		assert schema.getClass("ORDER").getProperty("id").getType() == OType.INTEGER;
		assert schema.getClass("ORDER").getProperty("date").getType() == OType.DATE;
		assert schema.getClass("ORDER").getProperty("customer").getType() == OType.STRING;
		assert schema.getClass("ORDER").getProperty("items").getType() == OType.EMBEDDEDLIST;
		assert schema.getClass("ORDER").getProperty("items").getLinkedClass().getName().equalsIgnoreCase("OrderItem");
		assert schema.getClass("ORDER").getProperty("totalPrice").getType() == OType.FLOAT;

		database.close();
	}

	@Test(dependsOnMethods = "checkSchema")
	public void checkSchemaApi() {
		database = new ODatabaseRecordFlat(url);
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
		database = new ODatabaseRecordFlat(url);
		database.open("admin", "admin");

		for (OClass cls : database.getMetadata().getSchema().classes()) {
			assert database.getClusterNameById(cls.getDefaultClusterId()) != null;
		}

		database.close();
	}
}
