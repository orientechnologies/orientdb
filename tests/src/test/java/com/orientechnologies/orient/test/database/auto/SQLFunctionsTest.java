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

import java.text.SimpleDateFormat;
import java.util.*;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-select")
public class SQLFunctionsTest {
  private ODatabaseDocument database;

  @Parameters(value = "url")
  public SQLFunctionsTest(String iURL) {
    database = new ODatabaseDocumentTx(iURL);
  }

  @Test
  public void queryMax() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select max(id) as max from Account")).execute();

    Assert.assertTrue(result.size() == 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("max"));
    }
  }

  @Test
  public void queryMaxInline() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select max(1,2,7,0,-2,3)")).execute();

    Assert.assertTrue(result.size() == 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("max"));

      Assert.assertEquals(((Number) d.field("max")).intValue(), 7);
    }
  }

  @Test
  public void queryMin() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select min(id) as min from Account")).execute();

    Assert.assertTrue(result.size() == 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("min"));

      Assert.assertEquals(((Number) d.field("min")).longValue(), 0l);
    }
  }

  @Test
  public void queryMinInline() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select min(1,2,7,0,-2,3)")).execute();

    Assert.assertTrue(result.size() == 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("min"));

      Assert.assertEquals(((Number) d.field("min")).intValue(), -2);
    }
  }

  @Test
  public void querySum() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select sum(id) as sum from Account")).execute();

    Assert.assertTrue(result.size() == 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("sum"));
    }
  }

  @Test
  public void queryCount() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select count(*) as total from Account")).execute();

    Assert.assertTrue(result.size() == 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("total"));
      Assert.assertTrue(((Number) d.field("total")).longValue() > 0);
    }
  }

  @Test
  public void queryCountWithConditions() {
    OClass indexed = database.getMetadata().getSchema().getOrCreateClass("Indexed");
    indexed.createProperty("key", OType.STRING);
    indexed.createIndex("keyed", OClass.INDEX_TYPE.NOTUNIQUE, "key");
    database.<ODocument> newInstance("Indexed").field("key", "one").save();
    database.<ODocument> newInstance("Indexed").field("key", "two").save();

    List<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select count(*) as total from Indexed where key > 'one'")).execute();

    Assert.assertTrue(result.size() == 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("total"));
      Assert.assertTrue(((Number) d.field("total")).longValue() > 0);
    }
  }

  @Test
  public void queryDistinct() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select distinct(name) as name from City")).execute();

    Assert.assertTrue(result.size() > 1);

    Set<String> cities = new HashSet<String>();
    for (ODocument city : result) {
      String cityName = (String) city.field("name");
      Assert.assertFalse(cities.contains(cityName));
      cities.add(cityName);
    }
  }

  @Test
  public void queryFunctionRenamed() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select distinct(name) from City")).execute();

    Assert.assertTrue(result.size() > 1);

    for (ODocument city : result)
      Assert.assertTrue(city.containsField("distinct"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void queryUnionAsAggregationNotRemoveDuplicates() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from City")).execute();
    int count = result.size();

    result = database.command(new OSQLSynchQuery<ODocument>("select union(name) as name from City")).execute();
    Collection<Object> citiesFound = (Collection<Object>) result.get(0).field("name");
    Assert.assertEquals(citiesFound.size(), count);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void querySetNotDuplicates() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select set(name) as name from City")).execute();

    Assert.assertTrue(result.size() == 1);

    Collection<Object> citiesFound = (Collection<Object>) result.get(0).field("name");
    Assert.assertTrue(citiesFound.size() > 1);

    Set<String> cities = new HashSet<String>();
    for (Object city : citiesFound) {
      Assert.assertFalse(cities.contains(city.toString()));
      cities.add(city.toString());
    }
  }

  @Test
  public void queryList() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select list(name) as names from City")).execute();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      List<Object> citiesFound = d.field("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  @Test
  public void querySet() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select set(name) as names from City")).execute();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Set<Object> citiesFound = d.field("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  @Test
  public void queryMap() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select map(name, country.name) as names from City"))
        .execute();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Map<Object, Object> citiesFound = d.field("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  @Test
  public void queryUnionAsInline() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select union(out, in) as edges from V")).execute();

    Assert.assertTrue(result.size() > 1);
    for (ODocument d : result) {
      Assert.assertEquals(d.fieldNames().length, 1);
      Assert.assertTrue(d.containsField("edges"));
    }
  }

  @Test
  public void queryComposedAggregates() {
    List<ODocument> result = database
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
      Assert.assertTrue(((Number) d.field("total")).longValue() >= ((Number) d.field("max")).longValue(),
          "Total " + d.field("total") + " max " + d.field("max"));
    }
  }

  @Test
  public void queryFormat() {
    List<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select format('%d - %s (%s)', nr, street, type, dummy ) as output from Account")).execute();

    Assert.assertTrue(result.size() > 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("output"));
    }
  }

  @Test
  public void querySysdateNoFormat() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select sysdate() as date from Account")).execute();

    Assert.assertTrue(result.size() > 1);
    Object lastDate = null;
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("date"));

      if (lastDate != null)
        d.field("date").equals(lastDate);

      lastDate = d.field("date");
    }
  }

  @Test
  public void querySysdateWithFormat() {
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
  }

  @Test
  public void queryDate() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select count(*) as tot from Account")).execute();
    Assert.assertEquals(result.size(), 1);
    int tot = ((Number) result.get(0).field("tot")).intValue();

    int updated = ((Number) database.command(new OCommandSQL("update Account set created = date()")).execute()).intValue();
    Assert.assertEquals(updated, tot);

    String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);

    result = database.command(
        new OSQLSynchQuery<ODocument>("select from Account where created <= date('" + dateFormat.format(new Date()) + "', '"
            + pattern + "')")).execute();

    Assert.assertEquals(result.size(), tot);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("created"));
    }
  }

  @Test(expectedExceptions = OCommandSQLParsingException.class)
  public void queryUndefinedFunction() {
    database.command(new OSQLSynchQuery<ODocument>("select blaaaa(salary) as max from Account")).execute();
  }

  @Test
  public void queryCustomFunction() {
    OSQLEngine.getInstance().registerFunction("bigger", new OSQLFunctionAbstract("bigger", 2, 2) {
      public String getSyntax() {
        return "bigger(<first>, <second>)";
      }

      public Object execute(OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParameters,
          OCommandContext iContext) {
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

    OSQLEngine.getInstance().unregisterFunction("bigger");
  }

  @Test
  public void queryAsLong() {
    long moreThanInteger = 1 + (long) Integer.MAX_VALUE;
    String sql = "select numberString.asLong() as value from ( select '" + moreThanInteger
        + "' as numberString from Account ) limit 1";
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>(sql)).execute();

    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("value"));
      Assert.assertTrue(d.field("value") instanceof Long);
      Assert.assertEquals(moreThanInteger, d.field("value"));
    }
  }

  @BeforeTest
  public void openDatabase() {
    database.open("admin", "admin");
  }

  @AfterTest
  public void closeDatabase() {
    database.close();
  }
}
