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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-select")
public class SQLSelectTest {
	private ODatabaseDocument	database;
	private ODocument					record;

	@Parameters(value = "url")
	public SQLSelectTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void queryNoDirtyResultset() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>(" select from Profile ")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertFalse(d.isDirty());
		}

		database.close();
	}

	@Test
	public void queryNoWhere() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>(" select from Profile ")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryParentesisAsRight() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>(
						"  select from Profile where ( name = 'Giuseppe' and ( name <> 'Napoleone' and nick is not null ))  ")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void querySingleAndDoubleQuotes() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where name = 'Giuseppe'"))
				.execute();

		final int count = result.size();
		Assert.assertTrue(result.size() != 0);

		result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where name = \"Giuseppe\"")).execute();
		Assert.assertTrue(result.size() != 0);
		Assert.assertEquals(result.size(), count);

		database.close();
	}

	@Test
	public void queryTwoParentesisConditions() {
		database.open("admin", "admin");

		List<ODocument> result = database
				.command(
						new OSQLSynchQuery<ODocument>(
								"select from Profile  where ( name = 'Giuseppe' and nick is not null ) or ( name = 'Napoleone' and nick is not null ) "))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void querySchemaAndLike() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select * from cluster:profile where name like 'G%'"))
				.execute();

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
			Assert.assertTrue(record.field("name").toString().startsWith("G"));
		}

		result = database.command(new OSQLSynchQuery<ODocument>("select * from cluster:profile where name like '%G%'")).execute();

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
			Assert.assertTrue(record.field("name").toString().contains("G"));
		}

		result = database.command(new OSQLSynchQuery<ODocument>("select * from cluster:profile where name like ?")).execute("%G%");

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
			Assert.assertTrue(record.field("name").toString().contains("G"));
		}

		database.close();
	}

	@Test
	public void queryLogicalCluster() throws ParseException {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select * from cluster:Order")).execute();

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);
		}

		database.close();
	}

	@Test
	public void queryContainsInEmbeddedSet() {
		database.open("admin", "admin");

		Set<String> tags = new HashSet<String>();
		tags.add("smart");
		tags.add("nice");

		ODocument doc = new ODocument(database, "Profile");
		doc.field("tags", tags);

		doc.save();

		List<ODocument> resultset = database.query(new OSQLSynchQuery<ODocument>("select from Profile where tags CONTAINS 'smart'"));

		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

		database.close();
	}

	@Test
	public void queryContainsInEmbeddedList() {
		database.open("admin", "admin");

		List<String> tags = new ArrayList<String>();
		tags.add("smart");
		tags.add("nice");

		ODocument doc = new ODocument(database, "Profile");
		doc.field("tags", tags);

		doc.save();

		List<ODocument> resultset = database.query(new OSQLSynchQuery<ODocument>("select from Profile where tags[0] = 'smart'"));

		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

		resultset = database.query(new OSQLSynchQuery<ODocument>("select from Profile where tags[0,1] CONTAINSALL ['smart','nice']"));

		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

		resultset = database.query(new OSQLSynchQuery<ODocument>("select from Profile where tags[0-1] CONTAINSALL ['smart','nice']"));

		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

		doc.delete();

		database.close();
	}

	@Test
	public void queryContainsInDocumentSet() {
		database.open("admin", "admin");

		HashSet<ODocument> coll = new HashSet<ODocument>();
		coll.add(new ODocument("name", "Luca", "surname", "Garulli"));
		coll.add(new ODocument("name", "Jay", "surname", "Miner"));

		ODocument doc = new ODocument(database, "Profile");
		doc.field("coll", coll, OType.EMBEDDEDSET);

		doc.save();

		List<ODocument> resultset = database.query(new OSQLSynchQuery<ODocument>(
				"select coll[name='Jay'] as value from Profile where coll is not null"));
		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).field("value").getClass(), ODocument.class);
		Assert.assertEquals(((ODocument) resultset.get(0).field("value")).field("name"), "Jay");

		doc.delete();

		database.close();
	}

	@Test
	public void queryContainsInDocumentList() {
		database.open("admin", "admin");

		List<ODocument> coll = new ArrayList<ODocument>();
		coll.add(new ODocument("name", "Luca", "surname", "Garulli"));
		coll.add(new ODocument("name", "Jay", "surname", "Miner"));

		ODocument doc = new ODocument(database, "Profile");
		doc.field("coll", coll, OType.EMBEDDEDLIST);

		doc.save();

		List<ODocument> resultset = database.query(new OSQLSynchQuery<ODocument>(
				"select coll[name='Jay'] as value from Profile where coll is not null"));
		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).field("value").getClass(), ODocument.class);
		Assert.assertEquals(((ODocument) resultset.get(0).field("value")).field("name"), "Jay");

		doc.delete();

		database.close();
	}

	@Test
	public void queryContainsInEmbeddedMapClassic() {
		database.open("admin", "admin");

		Map<String, ODocument> customReferences = new HashMap<String, ODocument>();
		customReferences.put("first", new ODocument("name", "Luca", "surname", "Garulli"));
		customReferences.put("second", new ODocument("name", "Jay", "surname", "Miner"));

		ODocument doc = new ODocument(database, "Profile");
		doc.field("customReferences", customReferences, OType.EMBEDDEDMAP);

		doc.save();

		List<ODocument> resultset = database.query(new OSQLSynchQuery<ODocument>(
				"select from Profile where customReferences CONTAINSKEY 'first'"));

		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

		resultset = database.query(new OSQLSynchQuery<ODocument>(
				"select from Profile where customReferences CONTAINSVALUE ( name like 'Ja%')"));

		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

		resultset = database.query(new OSQLSynchQuery<ODocument>(
				"select from Profile where customReferences[second]['name'] like 'Ja%'"));
		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

		resultset = database.query(new OSQLSynchQuery<ODocument>(
				"select customReferences[second]['name'] from Profile where customReferences[second]['name'] is not null"));
		Assert.assertEquals(resultset.size(), 1);

		resultset = database.query(new OSQLSynchQuery<ODocument>(
				"select customReferences[second]['name'] as value from Profile where customReferences[second]['name'] is not null"));
		Assert.assertEquals(resultset.size(), 1);

		doc.delete();

		database.close();
	}

	@Test
	public void queryContainsInEmbeddedMapNew() {
		database.open("admin", "admin");

		Map<String, ODocument> customReferences = new HashMap<String, ODocument>();
		customReferences.put("first", new ODocument("name", "Luca", "surname", "Garulli"));
		customReferences.put("second", new ODocument("name", "Jay", "surname", "Miner"));

		ODocument doc = new ODocument(database, "Profile");
		doc.field("customReferences", customReferences, OType.EMBEDDEDMAP);

		doc.save();

		List<ODocument> resultset = database.query(new OSQLSynchQuery<ODocument>(
				"select from Profile where customReferences.keys() CONTAINS 'first'"));

		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

		resultset = database.query(new OSQLSynchQuery<ODocument>(
				"select from Profile where customReferences.values() CONTAINS ( name like 'Ja%')"));

		Assert.assertEquals(resultset.size(), 1);
		Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

		doc.delete();

		database.close();
	}

	@Test
	public void queryCollectionContainsLowerCaseSubStringIgnoreCase() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>(
						"select * from cluster:animaltype where races contains (name.toLowerCase().subString(0,1) = 'e')")).execute();

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("animaltype"));
			Assert.assertNotNull(record.field("races"));

			Collection<ODocument> races = record.field("races");
			boolean found = false;
			for (ODocument race : races) {
				if (((String) race.field("name")).toLowerCase().substring(0, 1).equals("e")) {
					found = true;
					break;
				}
			}
			Assert.assertTrue(found);
		}

		database.close();
	}

	@Test
	public void queryCollectionContainsInRecords() {
		database.open("admin", "admin");

		record.setDatabase(database);
		record.reset();
		record.setClassName("Animal");
		record.field("name", "Cat");

		Collection<ODocument> races = new HashSet<ODocument>();
		races.add(((ODocument) database.newInstance("AnimalRace")).field("name", "European"));
		races.add(((ODocument) database.newInstance("AnimalRace")).field("name", "Siamese"));
		record.field("races", races);
		record.save();

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select * from cluster:animal where races contains (name in ['European','Asiatic'])"))
				.execute();

		boolean found = false;
		for (int i = 0; i < result.size() && !found; ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
			Assert.assertNotNull(record.field("races"));

			races = record.field("races");
			for (ODocument race : races) {
				if (((String) race.field("name")).equals("European") || ((String) race.field("name")).equals("Asiatic")) {
					found = true;
					break;
				}
			}
		}
		Assert.assertTrue(found);

		result = database.command(
				new OSQLSynchQuery<ODocument>("select * from cluster:animal where races contains (name in ['Asiatic','European'])"))
				.execute();

		found = false;
		for (int i = 0; i < result.size() && !found; ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
			Assert.assertNotNull(record.field("races"));

			races = record.field("races");
			for (ODocument race : races) {
				if (((String) race.field("name")).equals("European") || ((String) race.field("name")).equals("Asiatic")) {
					found = true;
					break;
				}
			}
		}
		Assert.assertTrue(found);

		result = database.command(
				new OSQLSynchQuery<ODocument>("select * from cluster:animal where races contains (name in ['aaa','bbb'])")).execute();
		Assert.assertEquals(result.size(), 0);

		result = database.command(
				new OSQLSynchQuery<ODocument>("select * from cluster:animal where races containsall (name in ['European','Asiatic'])"))
				.execute();
		Assert.assertEquals(result.size(), 0);

		result = database.command(
				new OSQLSynchQuery<ODocument>("select * from cluster:animal where races containsall (name in ['European','Siamese'])"))
				.execute();
		Assert.assertEquals(result.size(), 1);

		record.delete();

		database.close();
	}

	@Test
	public void queryCollectionInNumbers() {
		database.open("admin", "admin");

		record.reset();
		record.setClassName("Animal");
		record.field("name", "Cat");

		Collection<Integer> rates = new HashSet<Integer>();
		rates.add(100);
		rates.add(200);
		record.field("rates", rates);

		record.save();

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select * from cluster:animal where rates in [100,105]")).execute();

		boolean found = false;
		for (int i = 0; i < result.size() && !found; ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
			Assert.assertNotNull(record.field("rates"));

			rates = record.field("rates");
			for (Integer rate : rates) {
				if (rate == 100 || rate == 105) {
					found = true;
					break;
				}
			}
		}
		Assert.assertTrue(found);

		result = database.command(new OSQLSynchQuery<ODocument>("select * from cluster:animal where rates in [200,10333]")).execute();

		found = false;
		for (int i = 0; i < result.size() && !found; ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
			Assert.assertNotNull(record.field("rates"));

			rates = record.field("rates");
			for (Integer rate : rates) {
				if (rate == 100 || rate == 105) {
					found = true;
					break;
				}
			}
		}
		Assert.assertTrue(found);

		result = database.command(new OSQLSynchQuery<ODocument>("select * from cluster:animal where rates in [500]")).execute();
		Assert.assertEquals(result.size(), 0);

		result = database.command(new OSQLSynchQuery<ODocument>("select * from cluster:animal where rates in 500")).execute();
		Assert.assertEquals(result.size(), 0);

		result = database.command(new OSQLSynchQuery<ODocument>("select * from cluster:animal where rates in [100])")).execute();
		Assert.assertEquals(result.size(), 1);

		record.delete();

		database.close();
	}

	@Test
	public void queryWhereRidDirectMatching() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select * from OUser where roles in #3:0")).execute();

		Assert.assertEquals(result.size(), 1);

		database.close();
	}

	@Test
	public void queryInAsParameter() {
		database.open("admin", "admin");

		List<ODocument> roles = database.query(new OSQLSynchQuery<Object>("select from orole"));
		roles.remove(0);

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select * from OUser where roles in ?")).execute(roles);

		Assert.assertEquals(result.size(), 2);

		database.close();
	}

	@Test
	public void queryAnyOperator() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where any() like 'N%'")).execute();

		Assert.assertTrue(result.size() > 0);

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Profile"));

			boolean found = false;
			for (Object fieldValue : record.fieldValues()) {
				if (fieldValue != null && fieldValue.toString().startsWith("N")) {
					found = true;
					break;
				}
			}
			Assert.assertTrue(found);
		}

		database.close();
	}

	@Test
	public void queryTraverseAnyOperator() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where any() traverse(0,3,any()) ( any().indexOf('Navona') > -1 )"))
				.execute();

		Assert.assertTrue(result.size() > 0);

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Profile"));
		}

		database.close();
	}

	@Test
	public void queryTraverseAndClass() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where any() traverse(0,7) (@class = 'City')")).execute();

		Assert.assertTrue(result.size() > 0);

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Profile"));
		}

		database.close();
	}

	@Test
	public void queryTraverseInfiniteLevelOperator() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where any() traverse(0,-1) ( any().indexOf('Navona') > -1 )")).execute();

		Assert.assertTrue(result.size() > 0);

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Profile"));
		}

		database.close();
	}

	@Test
	public void queryTraverseEdges() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>(
						"select from Profile where any() traverse(0,-1,'followers,followings') ( followers.size() > 0 )")).execute();

		Assert.assertTrue(result.size() > 0);

		database.close();
	}

	@Test
	public void queryAllOperator() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Account where all() is null")).execute();

		Assert.assertTrue(result.size() == 0);

		database.close();
	}

	@Test
	public void queryOrderBy() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile order by name")).execute();

		Assert.assertTrue(result.size() != 0);

		String lastName = null;
		boolean isNullSegment = false; // NULL VALUES AT THE END!
		for (ODocument d : result) {
			final String fieldValue = d.field("name");
			if (fieldValue == null)
				isNullSegment = true;
			else
				Assert.assertFalse(isNullSegment);

			if (lastName != null && fieldValue != null)
				Assert.assertTrue(fieldValue.compareTo(lastName) >= 0);
			lastName = fieldValue;
		}

		database.close();
	}

	@Test
	public void queryOrderByAndLimit() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile order by name limit 2")).execute();

		Assert.assertTrue(result.size() <= 2);

		String lastName = null;
		for (ODocument d : result) {
			if (lastName != null && d.field("name") != null)
				Assert.assertTrue(((String) d.field("name")).compareTo(lastName) >= 0);
			lastName = d.field("name");
		}

		database.close();
	}

	@Test
	public void queryConditionAndOrderBy() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where name is not null order by name")).execute();

		Assert.assertTrue(result.size() != 0);

		String lastName = null;
		for (ODocument d : result) {
			if (lastName != null && d.field("name") != null)
				Assert.assertTrue(((String) d.field("name")).compareTo(lastName) >= 0);
			lastName = d.field("name");
		}

		database.close();
	}

	@Test
	public void queryConditionsAndOrderBy() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where name is not null order by name desc, id asc")).execute();

		Assert.assertTrue(result.size() != 0);

		String lastName = null;
		for (ODocument d : result) {
			if (lastName != null && d.field("name") != null)
				Assert.assertTrue(((String) d.field("name")).compareTo(lastName) <= 0);
			lastName = d.field("name");
		}

		database.close();
	}

	@Test
	public void queryRecordTargetRid() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from 10:0")).execute();

		Assert.assertTrue(result.size() == 1);

		for (ODocument d : result) {
			Assert.assertTrue(d.getIdentity().toString().equals("#10:0"));
		}

		database.close();
	}

	@Test
	public void queryRecordTargetRids() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>(" select from [10:0, 10:1]")).execute();

		Assert.assertTrue(result.size() == 2);

		Assert.assertTrue(result.get(0).getIdentity().toString().equals("#10:0"));
		Assert.assertTrue(result.get(1).getIdentity().toString().equals("#10:1"));

		database.close();
	}

	@Test
	public void queryRecordAttribRid() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @rid = #10:0")).execute();

		Assert.assertTrue(result.size() == 1);

		for (ODocument d : result) {
			Assert.assertTrue(d.getIdentity().toString().equals("#10:0"));
		}

		database.close();
	}

	@Test
	public void queryRecordAttribClass() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @class = 'Profile'"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.getClassName().equals("Profile"));
		}

		database.close();
	}

	@Test
	public void queryRecordAttribVersion() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @version > 0")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.getVersion() > 0);
		}

		database.close();
	}

	@Test
	public void queryRecordAttribSize() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @size >= 50")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.toStream().length >= 50);
		}

		database.close();
	}

	@Test
	public void queryRecordAttribType() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @type = 'document'"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryWrongOperator() {
		database.open("admin", "admin");

		try {
			database.query(new OSQLSynchQuery<ODocument>("select from Profile where name like.toLowerCase() '%Jay%'"));
			Assert.assertFalse(true);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		database.close();
	}

	@Test
	public void queryEscaping() {
		database.open("admin", "admin");

		database.query(new OSQLSynchQuery<ODocument>("select from Profile where name like '%\\'Jay%'"));

		database.close();
	}

	@Test
	public void queryWithLimit() {
		database.open("admin", "admin");

		Assert.assertEquals(database.query(new OSQLSynchQuery<ODocument>("select from Profile limit 3")).size(), 3);

		database.close();
	}

	@SuppressWarnings("unused")
	@Test
	public void testRecordNumbers() {
		database.open("admin", "admin");

		long tot = database.countClass("OGraphVertex");

		int count = 0;
		for (ODocument record : database.browseClass("OGraphVertex")) {
			count++;
		}

		Assert.assertEquals(count, tot);

		Assert.assertTrue(database.query(new OSQLSynchQuery<ODocument>("select from OGraphVertex")).size() >= tot);

		database.close();
	}

	@Test
	public void queryWithRange() {
		database.open("admin", "admin");

		final String query = "select from Profile limit 3";

		List<ODocument> resultset = database.query(new OSQLSynchQuery<ODocument>(query));
		Assert.assertFalse(resultset.isEmpty());
		Assert.assertTrue(resultset.size() <= 3);

		while (!resultset.isEmpty()) {
			ORID last = resultset.get(resultset.size() - 1).getIdentity();

			resultset = database.query(new OSQLSynchQuery<ODocument>(query + " range " + last.next()));
			Assert.assertTrue(resultset.size() <= 3);

			for (ODocument d : resultset)
				Assert.assertTrue(d.getIdentity().getClusterId() >= last.getClusterId()
						&& d.getIdentity().getClusterPosition() > last.getClusterPosition());
		}

		database.close();
	}

	@Test
	public void queryWithPagination() {
		database.open("admin", "admin");

		final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile LIMIT 3");

		ORID last = new ORecordId();

		for (List<ODocument> resultset = database.query(query); !resultset.isEmpty(); resultset = query.execute()) {
			Assert.assertTrue(resultset.size() <= 3);

			for (ODocument d : resultset) {
				Assert.assertTrue(d.getIdentity().getClusterId() < 0 || (d.getIdentity().getClusterId() >= last.getClusterId())
						&& d.getIdentity().getClusterPosition() > last.getClusterPosition());
			}

			last = resultset.get(resultset.size() - 1).getIdentity();
		}

		database.close();
	}

	@Test
	public void queryBetween() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select * from account where nr between 10 and 20"))
				.execute();

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			Assert.assertTrue(((Integer) record.field("nr")) >= 10 && ((Integer) record.field("nr")) <= 20);
		}

		database.close();
	}

	@Test
	public void queryParenthesisInStrings() {
		database.open("admin", "admin");

		Assert.assertNotNull(database.command(new OCommandSQL("INSERT INTO account (name) VALUES ('test (demo)')")).execute());

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select * from account where name = 'test (demo)'"))
				.execute();

		Assert.assertEquals(result.size(), 1);

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);
			Assert.assertEquals(record.field("name"), "test (demo)");
		}

		database.close();
	}

	@Test
	public void queryMathOperators() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select * from account where id < 3 + 4")).execute();
		Assert.assertFalse(result.isEmpty());
		for (int i = 0; i < result.size(); ++i)
			Assert.assertTrue(((Integer) result.get(i).field("id")) < 3 + 4);

		result = database.command(new OSQLSynchQuery<ODocument>("select * from account where id < 10 - 3")).execute();
		Assert.assertFalse(result.isEmpty());
		for (int i = 0; i < result.size(); ++i)
			Assert.assertTrue(((Integer) result.get(i).field("id")) < 10 - 3);

		result = database.command(new OSQLSynchQuery<ODocument>("select * from account where id < 3 * 2")).execute();
		Assert.assertFalse(result.isEmpty());
		for (int i = 0; i < result.size(); ++i)
			Assert.assertTrue(((Integer) result.get(i).field("id")) < 3 * 2);

		result = database.command(new OSQLSynchQuery<ODocument>("select * from account where id < 120 / 20")).execute();
		Assert.assertFalse(result.isEmpty());
		for (int i = 0; i < result.size(); ++i)
			Assert.assertTrue(((Integer) result.get(i).field("id")) < 120 / 20);

		result = database.command(new OSQLSynchQuery<ODocument>("select * from account where id < 27 % 10")).execute();
		Assert.assertFalse(result.isEmpty());
		for (int i = 0; i < result.size(); ++i)
			Assert.assertTrue(((Integer) result.get(i).field("id")) < 27 % 10);

		result = database.command(new OSQLSynchQuery<ODocument>("select * from account where id = id * 1")).execute();
		Assert.assertFalse(result.isEmpty());

		List<ODocument> result2 = database.command(new OSQLSynchQuery<ODocument>("select count(*) as tot from account where id >= 0"))
				.execute();
		Assert.assertEquals(result.size(), ((Number) result2.get(0).field("tot")).intValue());

		database.close();
	}

	@Test
	public void testBetweenWithParameters() {
		database.open("admin", "admin");

		final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select * from company where id between ? and ?"),
				4, 7);

		Assert.assertEquals(result.size(), 4);

		final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
		for (final ODocument record : result) {
			resultsList.remove(record.<Integer> field("id"));
		}

		database.close();
	}

	@Test
	public void testInWithParameters() {
		database.open("admin", "admin");

		final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select * from company where id in [?, ?, ?, ?]"),
				4, 5, 6, 7);

		Assert.assertEquals(result.size(), 4);

		final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
		for (final ODocument record : result) {
			resultsList.remove(record.<Integer> field("id"));
		}

		database.close();
	}

}
