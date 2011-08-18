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
import java.util.Collection;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@Test(groups = "dictionary")
public class TransactionOptimisticTest {
	private String	url;

	@Parameters(value = "url")
	public TransactionOptimisticTest(String iURL) {
		url = iURL;
	}

	@Test
	public void testTransactionOptimisticRollback() throws IOException {
		ODatabaseFlat db1 = new ODatabaseFlat(url);
		db1.open("admin", "admin");

		long rec = db1.countClusterElements("binary");

		db1.begin();

		ORecordFlat record1 = new ORecordFlat(db1);
		record1.value("This is the first version").save();

		db1.rollback();

		Assert.assertEquals(db1.countClusterElements("binary"), rec);

		db1.close();
	}

	@Test(dependsOnMethods = "testTransactionOptimisticRollback")
	public void testTransactionOptimisticCommit() throws IOException {
		ODatabaseFlat db1 = new ODatabaseFlat(url);
		db1.open("admin", "admin");

		long tot = db1.countClusterElements("binary");

		db1.begin();

		ORecordFlat record1 = new ORecordFlat(db1);
		record1.value("This is the first version").save("binary");

		db1.commit();

		Assert.assertEquals(db1.countClusterElements("binary"), tot + 1);

		db1.close();
	}

	@Test(dependsOnMethods = "testTransactionOptimisticCommit")
	public void testTransactionOptimisticCuncurrentException() throws IOException {
		ODatabaseFlat db1 = new ODatabaseFlat(url);
		db1.open("admin", "admin");

		ODatabaseFlat db2 = new ODatabaseFlat(url);
		db2.open("admin", "admin");

		ORecordFlat record1 = new ORecordFlat(db1);
		record1.value("This is the first version").save();

		try {
			db1.begin();

			// RE-READ THE RECORD
			record1.load();
			ORecordFlat record2 = db2.load(record1.getIdentity());

			record2.value("This is the second version").save();
			record1.value("This is the third version").save();

			db1.commit();

			Assert.assertTrue(false);

		} catch (OConcurrentModificationException e) {
			Assert.assertTrue(true);
			db1.rollback();

		} finally {

			db1.close();
			db2.close();
		}
	}

	@Test(dependsOnMethods = "testTransactionOptimisticCuncurrentException")
	public void testTransactionOptimisticCacheMgmt1Db() throws IOException {
		ODatabaseFlat db = new ODatabaseFlat(url);
		db.open("admin", "admin");

		ORecordFlat record = new ORecordFlat(db);
		record.value("This is the first version").save();

		try {
			db.begin();

			// RE-READ THE RECORD
			record.load();
			int v1 = record.getVersion();
			record.value("This is the second version").save();
			db.commit();

			record.reload();
			Assert.assertEquals(record.getVersion(), v1 + 1);
			Assert.assertTrue(record.value().contains("second"));
		} finally {

			db.close();
		}
	}

	@Test(dependsOnMethods = "testTransactionOptimisticCacheMgmt1Db")
	public void testTransactionOptimisticCacheMgmt2Db() throws IOException {
		ODatabaseFlat db1 = new ODatabaseFlat(url);
		db1.open("admin", "admin");

		ODatabaseFlat db2 = new ODatabaseFlat(url);
		db2.open("admin", "admin");

		ORecordFlat record1 = new ORecordFlat(db1);
		record1.value("This is the first version").save();

		try {
			db1.begin();

			// RE-READ THE RECORD
			record1.load();
			int v1 = record1.getVersion();
			record1.value("This is the second version").save();

			db1.commit();

			ORecordFlat record2 = db2.load(record1.getIdentity());
			Assert.assertEquals(record2.getVersion(), v1 + 1);
			Assert.assertTrue(record2.value().contains("second"));

		} finally {

			db1.close();
			db2.close();
		}
	}

	@Test(dependsOnMethods = "testTransactionOptimisticCacheMgmt2Db")
	public void testTransactionMultipleRecords() throws IOException {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
		db.open("admin", "admin");

		long totalAccounts = db.countClusterElements("Account");

		String json = "{ \"@class\": \"Account\", \"type\": \"Residence\", \"street\": \"Piazza di Spagna\"}";

		db.begin(TXTYPE.OPTIMISTIC);
		for (int g = 0; g < 1000; g++) {
			ODocument doc = new ODocument(db, "Account");
			doc.fromJSON(json);
			doc.field("nr", g);

			doc.save();
		}
		db.commit();

		Assert.assertEquals(db.countClusterElements("Account"), totalAccounts + 1000);

		db.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void createGraphInTx() {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
		db.open("admin", "admin");

		db.begin();

		ODocument kim = new ODocument(db, "Profile").field("name", "Kim").field("surname", "Bauer");
		ODocument teri = new ODocument(db, "Profile").field("name", "Teri").field("surname", "Bauer");
		ODocument jack = new ODocument(db, "Profile").field("name", "Jack").field("surname", "Bauer");

		((HashSet<ODocument>) jack.field("following", new HashSet<ODocument>()).field("following")).add(kim);
		((HashSet<ODocument>) kim.field("following", new HashSet<ODocument>()).field("following")).add(teri);
		((HashSet<ODocument>) teri.field("following", new HashSet<ODocument>()).field("following")).add(jack);

		jack.save();

		db.commit();

		db.close();
		db.open("admin", "admin");

		ODocument loadedJack = db.load(jack.getIdentity());
		Assert.assertEquals(loadedJack.field("name"), "Jack");
		Collection<ODocument> jackFollowings = loadedJack.field("following");
		Assert.assertNotNull(jackFollowings.size());
		Assert.assertEquals(jackFollowings.size(), 1);

		ODocument loadedKim = jackFollowings.iterator().next();
		Assert.assertEquals(loadedKim.field("name"), "Kim");
		Collection<ODocument> kimFollowings = loadedKim.field("following");
		Assert.assertNotNull(kimFollowings.size());
		Assert.assertEquals(kimFollowings.size(), 1);

		ODocument loadedTeri = kimFollowings.iterator().next();
		Assert.assertEquals(loadedTeri.field("name"), "Teri");
		Collection<ODocument> teriFollowings = loadedTeri.field("following");
		Assert.assertNotNull(teriFollowings.size());
		Assert.assertEquals(teriFollowings.size(), 1);

		Assert.assertEquals(teriFollowings.iterator().next().field("name"), "Jack");

		db.close();
	}
//
//	@SuppressWarnings("unchecked")
//	@Test
//	public void createGraphInTxWithSchemaDefined() {
//		ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
//		db.open("admin", "admin");
//
//		OClass profileClass = db.getMetadata().getSchema().getClass("Profile");
//		profileClass.createProperty("following", OType.LINKSET, profileClass);
//
//		db.begin();
//
//		ODocument kim = new ODocument(db, "Profile").field("name", "Kim").field("surname", "Bauer");
//		ODocument teri = new ODocument(db, "Profile").field("name", "Teri").field("surname", "Bauer");
//		ODocument jack = new ODocument(db, "Profile").field("name", "Jack").field("surname", "Bauer");
//
//		((HashSet<ODocument>) jack.field("following", new HashSet<ODocument>()).field("following")).add(kim);
//		((HashSet<ODocument>) kim.field("following", new HashSet<ODocument>()).field("following")).add(teri);
//		((HashSet<ODocument>) teri.field("following", new HashSet<ODocument>()).field("following")).add(jack);
//
//		jack.save();
//
//		db.commit();
//
//		db.close();
//		db.open("admin", "admin");
//
//		ODocument loadedJack = db.load(jack.getIdentity());
//		Assert.assertEquals(loadedJack.field("name"), "Jack");
//		Collection<ODocument> jackFollowings = loadedJack.field("following");
//		Assert.assertNotNull(jackFollowings.size());
//		Assert.assertEquals(jackFollowings.size(), 1);
//
//		ODocument loadedKim = jackFollowings.iterator().next();
//		Assert.assertEquals(loadedKim.field("name"), "Kim");
//		Collection<ODocument> kimFollowings = loadedKim.field("following");
//		Assert.assertNotNull(kimFollowings.size());
//		Assert.assertEquals(kimFollowings.size(), 1);
//
//		ODocument loadedTeri = kimFollowings.iterator().next();
//		Assert.assertEquals(loadedTeri.field("name"), "Teri");
//		Collection<ODocument> teriFollowings = loadedTeri.field("following");
//		Assert.assertNotNull(teriFollowings.size());
//		Assert.assertEquals(teriFollowings.size(), 1);
//
//		Assert.assertEquals(teriFollowings.iterator().next().field("name"), "Jack");
//
//		db.close();
//	}
}
