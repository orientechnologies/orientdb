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

import java.lang.reflect.Field;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.test.domain.base.IdObject;
import com.orientechnologies.orient.test.domain.base.Instrument;
import com.orientechnologies.orient.test.domain.base.Musician;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Company;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.inheritance.InheritanceTestClass;
import com.orientechnologies.orient.test.domain.inheritance.InheritanceTestSuperclass;

@Test(groups = { "crud", "object" }, sequential = true)
public class CRUDObjectInheritanceTest {
	protected static final int	TOT_RECORDS	= 10;
	protected long							startRecordNumber;
	private OObjectDatabaseTx		database;
	private City								redmond			= new City(new Country("Washington"), "Redmond");

	@Parameters(value = "url")
	public CRUDObjectInheritanceTest(String iURL) {
		database = new OObjectDatabaseTx(iURL);
	}

	@Test
	public void create() {
		database.open("admin", "admin");

		database.command(new OCommandSQL("delete from Company")).execute();

		startRecordNumber = database.countClusterElements("Company");

		Company company;

		for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
			company = database.newInstance(Company.class, (int) i, "Microsoft" + i);
			company.setEmployees((int) (100000 + i));
			company.getAddresses().add(new Address("Headquarter", redmond, "WA 98073-9717"));
			database.save(company);
		}

		database.close();
	}

	@Test(dependsOnMethods = "create")
	public void testCreate() {
		database.open("admin", "admin");

		Assert.assertEquals(database.countClusterElements("Company") - startRecordNumber, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "testCreate")
	public void queryByBaseType() {
		database.open("admin", "admin");

		final List<Account> result = database.query(new OSQLSynchQuery<Account>("select from Company where name.length() > 0"));

		Assert.assertTrue(result.size() > 0);
		Assert.assertEquals(result.size(), TOT_RECORDS);

		int companyRecords = 0;
		Account account;
		for (int i = 0; i < result.size(); ++i) {
			account = result.get(i);

			if (account instanceof Company)
				companyRecords++;

			Assert.assertNotSame(account.getName().length(), 0);
		}

		Assert.assertEquals(companyRecords, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "queryByBaseType")
	public void queryPerSuperType() {
		database.open("admin", "admin");

		final List<Company> result = database.query(new OSQLSynchQuery<ODocument>("select * from Company where name.length() > 0"));

		Assert.assertTrue(result.size() == TOT_RECORDS);

		Company account;
		for (int i = 0; i < result.size(); ++i) {
			account = result.get(i);
			Assert.assertNotSame(account.getName().length(), 0);
		}

		database.close();
	}

	@Test(dependsOnMethods = "queryPerSuperType")
	public void deleteFirst() {
		database.open("admin", "admin");

		startRecordNumber = database.countClusterElements("Company");

		// DELETE ALL THE RECORD IN THE CLUSTER
		for (Object obj : database.browseCluster("Company")) {
			database.delete(obj);
			break;
		}

		Assert.assertEquals(database.countClusterElements("Company"), startRecordNumber - 1);

		database.close();
	}

	@Test(dependsOnMethods = "deleteFirst")
	public void testIdFieldInheritance() {
		database.open("admin", "admin");

		database.getEntityManager().registerEntityClass(Musician.class);
		database.getEntityManager().registerEntityClass(Instrument.class);
		database.getEntityManager().registerEntityClass(IdObject.class);
		Field idField = OObjectEntitySerializer.getIdField(IdObject.class);
		Assert.assertNotNull(idField);
		Field musicianIdField = OObjectEntitySerializer.getIdField(Musician.class);
		Assert.assertNotNull(musicianIdField);
		Assert.assertEquals(idField, musicianIdField);
		Field instrumentIdField = OObjectEntitySerializer.getIdField(Instrument.class);
		Assert.assertNotNull(instrumentIdField);
		Assert.assertEquals(idField, instrumentIdField);
		Assert.assertEquals(instrumentIdField, musicianIdField);
		idField = OObjectEntitySerializer.getIdField(IdObject.class);
		database.close();
	}

	@Test(dependsOnMethods = "testIdFieldInheritance")
	public void testIdFieldInheritanceFirstSubClass() {
		database.open("admin", "admin");

		database.getEntityManager().registerEntityClass(InheritanceTestClass.class);
		database.getEntityManager().registerEntityClass(InheritanceTestSuperclass.class);
		InheritanceTestSuperclass a = database.newInstance(InheritanceTestSuperclass.class);
		InheritanceTestSuperclass b = database.newInstance(InheritanceTestClass.class);
		database.save(a);
		database.save(b);

		final List<InheritanceTestSuperclass> result1 = database.query(new OSQLSynchQuery<InheritanceTestSuperclass>(
				"select from InheritanceTestSuperclass"));
		Assert.assertEquals(2, result1.size());
		database.close();
	}

}
