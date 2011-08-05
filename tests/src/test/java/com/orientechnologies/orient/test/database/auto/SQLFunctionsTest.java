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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-select")
public class SQLFunctionsTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public SQLFunctionsTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void queryMax() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select max(id) as max from Account")).execute();

		Assert.assertTrue(result.size() == 1);
		for (ODocument d : result) {
			Assert.assertNotNull(d.field("max"));
		}

		database.close();
	}

	@Test
	public void queryMin() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select min(id) as min from Account")).execute();

		Assert.assertTrue(result.size() == 1);
		for (ODocument d : result) {
			Assert.assertNotNull(d.field("min"));

			Assert.assertEquals(((Number) d.field("min")).longValue(), 0l);
		}

		database.close();
	}

	@Test
	public void querySum() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select sum(id) as sum from Account")).execute();

		Assert.assertTrue(result.size() == 1);
		for (ODocument d : result) {
			Assert.assertNotNull(d.field("sum"));
		}

		database.close();
	}

	@Test
	public void queryCount() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select count(*) as total from Account")).execute();

		Assert.assertTrue(result.size() == 1);
		for (ODocument d : result) {
			Assert.assertNotNull(d.field("total"));
			Assert.assertTrue(((Number) d.field("total")).longValue() > 0);
		}

		database.close();
	}

	@Test
	public void queryDistinct() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select distinct(name) as name from City")).execute();

		Assert.assertTrue(result.size() > 1);

		Set<String> cities = new HashSet<String>();
		for (ODocument city : result) {
			String cityName = (String) city.field("name");
			Assert.assertFalse(cities.contains(cityName));
			cities.add(cityName);
		}

		database.close();
	}

	@Test
	public void queryFunctionRenamed() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select distinct(name) from City")).execute();

		Assert.assertTrue(result.size() > 1);

		for (ODocument city : result)
			Assert.assertTrue(city.containsField("distinct"));

		database.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void queryUnionAsAggregation() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select union(name) as name from City")).execute();

		Assert.assertTrue(result.size() == 1);

		Collection<Object> citiesFound = (Collection<Object>) result.get(0).field("name");
		Assert.assertTrue(citiesFound.size() > 1);

		Set<String> cities = new HashSet<String>();
		for (Object city : citiesFound) {
			Assert.assertFalse(cities.contains(city.toString()));
			cities.add(city.toString());
		}

		database.close();
	}

	@Test
	public void queryUnionAsInline() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select union(out, in) as edges from OGraphVertex"))
				.execute();

		Assert.assertTrue(result.size() > 1);
		for (ODocument d : result) {
			Assert.assertEquals(d.fieldNames().length, 1);
			Assert.assertTrue(d.containsField("edges"));
		}

		database.close();
	}

	@Test
	public void queryComposedAggregates() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Account")).execute();

		result = database
				.command(
						new OSQLSynchQuery<ODocument>(
								"select MIN(id) as min, max(id) as max, AVG(id) as average, count(id) as total from Account")).execute();

		Assert.assertTrue(result.size() == 1);
		for (ODocument d : result) {
			Assert.assertNotNull(d.field("min"));
			Assert.assertNotNull(d.field("max"));
			Assert.assertNotNull(d.field("average"));
			Assert.assertNotNull(d.field("total"));

			Assert.assertTrue(((Number) d.field("max")).longValue() > ((Number) d.field("average")).longValue());
			Assert.assertTrue(((Number) d.field("average")).longValue() >= ((Number) d.field("min")).longValue());
			Assert.assertTrue(((Number) d.field("total")).longValue() >= ((Number) d.field("max")).longValue());
		}

		database.close();
	}

	@Test
	public void queryFormat() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select format('%d - %s (%s)', nr, street, type, dummy ) as output from Account")).execute();

		Assert.assertTrue(result.size() > 1);
		for (ODocument d : result) {
			Assert.assertNotNull(d.field("output"));
		}

		database.close();
	}

	@Test
	public void querySysdateNoFormat() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select sysdate() as date from Account")).execute();

		Assert.assertTrue(result.size() > 1);
		Object lastDate = null;
		for (ODocument d : result) {
			Assert.assertNotNull(d.field("date"));

			if (lastDate != null)
				d.field("date").equals(lastDate);

			lastDate = d.field("date");
		}

		database.close();
	}

	@Test
	public void querySysdateWithFormat() {
		database.open("admin", "admin");
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select sysdate('dd-MM-yyyy') as date from Account"))
				.execute();

		Assert.assertTrue(result.size() > 1);
		Object lastDate = null;
		for (ODocument d : result) {
			Assert.assertNotNull(d.field("date"));

			if (lastDate != null)
				d.field("date").equals(lastDate);

			lastDate = d.field("date");
		}

		database.close();
	}

	@Test(expectedExceptions = OCommandSQLParsingException.class)
	public void queryUndefinedFunction() {
		database.open("admin", "admin");
		try {
			database.command(new OSQLSynchQuery<ODocument>("select blaaaa(salary) as max from Account")).execute();
		} finally {
			database.close();
		}
	}

	@Test
	public void queryCustomFunction() {
		database.open("admin", "admin");

		OSQLEngine.getInstance().registerFunction("bigger", new OSQLFunctionAbstract("bigger", 2, 2) {
			public String getSyntax() {
				return "bigger(<first>, <second>)";
			}

			public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters) {
				if (iParameters[0] == null || iParameters[1] == null)
					// CHECK BOTH EXPECTED PARAMETERS
					return null;

				if (!(iParameters[0] instanceof Number) || !(iParameters[1] instanceof Number))
					// EXCLUDE IT FROM THE RESULT SET
					return null;

				// USE DOUBLE TO AVOID LOSS OF PRECISION
				final double v1 = ((Number) iParameters[0]).doubleValue();
				final double v2 = ((Number) iParameters[1]).doubleValue();

				return Math.max(v1, v2);
			}
		});

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Account where bigger(id,1000) = 1000"))
				.execute();

		Assert.assertTrue(result.size() != 0);
		for (ODocument d : result) {
			Assert.assertTrue((Integer) d.field("id") <= 1000);
		}

		OSQLEngine.getInstance().unregisterInlineFunction("bigger");
		database.close();
	}
}
