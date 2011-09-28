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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test(groups = "sql-findReferences")
public class SQLFindReferencesTest {

	private static final String	WORKPLACE	= "Workplace";
	private static final String	WORKER		= "Worker";
	private static final String	CAR				= "Car";

	private ODatabaseDocument		database;

	private ORID								carID;

	private ORID								johnDoeID;

	private ORID								janeDoeID;

	private ORID								chuckNorrisID;

	private ORID								jackBauerID;

	private ORID								ctuID;

	private ORID								fbiID;

	@Parameters(value = "url")
	public SQLFindReferencesTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void findSimpleReference() {
		if (database.isClosed())
			database.open("admin", "admin");

		Collection<ORID> result = database.command(new OCommandSQL("find references " + carID.toString())).execute();

		Assert.assertTrue(result.size() == 1);
		Assert.assertTrue(result.iterator().next().toString().equals(johnDoeID.toString()));

		result = database.command(new OCommandSQL("find references " + chuckNorrisID.toString())).execute();

		Assert.assertTrue(result.size() == 2);
		ORID rid = result.iterator().next();
		Assert.assertTrue(rid.toString().equals(ctuID.toString()) || rid.toString().equals(fbiID.toString()));
		rid = result.iterator().next();
		Assert.assertTrue(rid.toString().equals(ctuID.toString()) || rid.toString().equals(fbiID.toString()));

		result = database.command(new OCommandSQL("find references " + johnDoeID.toString())).execute();
		Assert.assertTrue(result.size() == 0);

		result.clear();
		result = null;

		database.close();
	}

	@Test
	public void findReferenceByClassAndClusters() {
		if (database.isClosed())
			database.open("admin", "admin");

		Collection<ORID> result = database.command(new OCommandSQL("find references " + janeDoeID.toString() + " [" + WORKPLACE + "]"))
				.execute();

		Assert.assertEquals(result.size(), 1);
		Assert.assertTrue(result.iterator().next().toString().equals(ctuID.toString()));

		result = database.command(
				new OCommandSQL("find references " + jackBauerID.toString() + " [" + WORKPLACE + ",cluster:" + CAR + "]")).execute();

		Assert.assertTrue(result.size() == 3);
		ORID rid = result.iterator().next();
		Assert.assertTrue(rid.toString().equals(ctuID.toString()) || rid.toString().equals(fbiID.toString())
				|| rid.toString().equals(carID.toString()));
		rid = result.iterator().next();
		Assert.assertTrue(rid.toString().equals(ctuID.toString()) || rid.toString().equals(fbiID.toString())
				|| rid.toString().equals(carID.toString()));
		rid = result.iterator().next();
		Assert.assertTrue(rid.toString().equals(ctuID.toString()) || rid.toString().equals(fbiID.toString())
				|| rid.toString().equals(carID.toString()));

		result = database.command(
				new OCommandSQL("find references " + johnDoeID.toString() + " [" + WORKPLACE + "," + CAR + ",cluster:" + WORKER + "]"))
				.execute();

		Assert.assertTrue(result.size() == 0);

		result.clear();
		result = null;

		database.close();
	}

	@BeforeClass
	public void createTestEnviroment() {
		if (database.isClosed())
			database.open("admin", "admin");
		createSchema();
		populateDatabase();
		database.close();
	}

	private void createSchema() {
		OClass worker = database.getMetadata().getSchema().createClass(WORKER);
		OClass workplace = database.getMetadata().getSchema().createClass(WORKPLACE);
		OClass car = database.getMetadata().getSchema().createClass(CAR);

		worker.createProperty("name", OType.STRING);
		worker.createProperty("surname", OType.STRING);
		worker.createProperty("colleagues", OType.LINKLIST, worker);
		worker.createProperty("car", OType.LINK, car);

		workplace.createProperty("name", OType.STRING);
		workplace.createProperty("boss", OType.LINK, worker);
		workplace.createProperty("workers", OType.LINKLIST, worker);

		car.createProperty("plate", OType.STRING);
		car.createProperty("owner", OType.LINK, worker);

		database.getMetadata().getSchema().save();
	}

	private void populateDatabase() {
		ODocument car = new ODocument(database, CAR);
		car.field("plate", "JINF223S");

		ODocument johnDoe = new ODocument(database, WORKER);
		johnDoe.field("name", "John");
		johnDoe.field("surname", "Doe");
		johnDoe.field("car", car);
		johnDoe.save();
		johnDoeID = johnDoe.getIdentity().copy();

		ODocument janeDoe = new ODocument(database, WORKER);
		janeDoe.field("name", "Jane");
		janeDoe.field("surname", "Doe");
		janeDoe.save();
		janeDoeID = janeDoe.getIdentity().copy();

		ODocument chuckNorris = new ODocument(database, WORKER);
		chuckNorris.field("name", "Chuck");
		chuckNorris.field("surname", "Norris");
		chuckNorris.save();
		chuckNorrisID = chuckNorris.getIdentity().copy();

		ODocument jackBauer = new ODocument(database, WORKER);
		jackBauer.field("name", "Jack");
		jackBauer.field("surname", "Bauer");
		jackBauer.save();
		jackBauerID = jackBauer.getIdentity().copy();

		ODocument ctu = new ODocument(database, WORKPLACE);
		ctu.field("name", "CTU");
		ctu.field("boss", jackBauer);
		List<ODocument> workplace1Workers = new ArrayList<ODocument>();
		workplace1Workers.add(chuckNorris);
		workplace1Workers.add(janeDoe);
		ctu.field("workers", workplace1Workers);
		ctu.save();
		ctuID = ctu.getIdentity().copy();

		ODocument fbi = new ODocument(database, WORKPLACE);
		fbi.field("name", "FBI");
		fbi.field("boss", chuckNorris);
		List<ODocument> workplace2Workers = new ArrayList<ODocument>();
		workplace2Workers.add(chuckNorris);
		workplace2Workers.add(jackBauer);
		fbi.field("workers", workplace2Workers);
		fbi.save();
		fbiID = fbi.getIdentity().copy();

		car.field("owner", jackBauer);
		car.save();
		carID = car.getIdentity().copy();
	}

	@AfterClass
	public void deleteTestEnviroment() {
		if (database.isClosed())
			database.open("admin", "admin");
		carID.reset();
		carID = null;
		johnDoeID.reset();
		johnDoeID = null;
		janeDoeID.reset();
		janeDoeID = null;
		chuckNorrisID.reset();
		chuckNorrisID = null;
		jackBauerID.reset();
		jackBauerID = null;
		ctuID.reset();
		ctuID = null;
		fbiID.reset();
		fbiID = null;
		deleteSchema();
		database.close();
	}

	private void deleteSchema() {
		dropClass(CAR);
		dropClass(WORKER);
		dropClass(WORKPLACE);
	}

	private void dropClass(String iClass) {
		OCommandSQL dropClassCommand = new OCommandSQL("drop class " + iClass);
		database.command(dropClassCommand).execute();
		database.getMetadata().getSchema().save();
		database.getMetadata().getSchema().reload();
		database.reload();
		while (database.getMetadata().getSchema().existsClass(iClass)) {
			database.getMetadata().getSchema().dropClass(iClass);
			database.getMetadata().getSchema().save();
			database.reload();
		}
		while (database.getClusterIdByName(iClass) > -1) {
			database.dropCluster(iClass);
			database.reload();
		}
	}

}
