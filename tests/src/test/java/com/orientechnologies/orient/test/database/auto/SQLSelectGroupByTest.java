/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-select")
public class SQLSelectGroupByTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLSelectGroupByTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    if (!database.getMetadata().getSchema().existsClass("Account"))
      database.getMetadata().getSchema().createClass("Account");
  }

  @Test
  public void queryGroupByBasic() {
    List<OResult> result =
        database.command("select location from Account group by location").stream().toList();

    Assert.assertTrue(result.size() > 1);
    Set<Object> set = new HashSet<Object>();
    for (OResult d : result) set.add(d.getProperty("location"));
    Assert.assertEquals(result.size(), set.size());
  }

  @Test
  public void queryGroupByLimit() {
    List<OResult> result =
        database.command("select location from Account group by location limit 2").stream()
            .toList();

    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void queryGroupByCount() {
    List<OResult> result =
        database.command("select count(*) from Account group by location").stream().toList();

    Assert.assertTrue(result.size() > 1);
  }

  @Test
  public void queryGroupByAndOrderBy() {
    List<OResult> result =
        database
            .command("select location from Account group by location order by location")
            .stream()
            .toList();

    Assert.assertTrue(result.size() > 1);
    String last = null;
    for (OResult d : result) {
      if (last != null) Assert.assertTrue(last.compareTo((String) d.getProperty("location")) < 0);
      last = d.getProperty("location");
    }

    result =
        database
            .command("select location from Account group by location order by location desc")
            .stream()
            .toList();

    Assert.assertTrue(result.size() > 1);
    last = null;
    for (OResult d : result) {
      Object current = d.getProperty("location");
      if (current != null) {
        if (last != null) Assert.assertTrue(last.compareTo((String) current) > 0);
      }
      last = d.getProperty("location");
    }
  }

  @Test
  public void queryGroupByAndWithNulls() {
    // INSERT WITH NO LOCATION (AS NULL)
    database.command("create class GroupByTest extends V").close();
    try {
      database.command("insert into GroupByTest set testNull = true").close();
      database.command("insert into GroupByTest set location = 'Rome'").close();
      database.command("insert into GroupByTest set location = 'Austin'").close();
      database.command("insert into GroupByTest set location = 'Austin'").close();

      final List<OResult> result =
          database.command("select location, count(*) from GroupByTest group by location").stream()
              .toList();

      Assert.assertEquals(result.size(), 3);

      boolean foundNullGroup = false;
      for (OResult d : result) {
        if (d.getProperty("location") == null) {
          Assert.assertFalse(foundNullGroup);
          foundNullGroup = true;
        }
      }

      Assert.assertTrue(foundNullGroup);
    } finally {
      database.command("delete vertex GroupByTest").close();
      database.command("drop class GroupByTest UNSAFE").close();
    }
  }

  @Test
  public void queryGroupByNoNulls() {
    database.command("create class GroupByTest extends V").close();
    try {
      database.command("insert into GroupByTest set location = 'Rome'").close();
      database.command("insert into GroupByTest set location = 'Austin'").close();
      database.command("insert into GroupByTest set location = 'Austin'").close();

      final List<OResult> result =
          database.command("select location, count(*) from GroupByTest group by location").stream()
              .toList();

      Assert.assertEquals(result.size(), 2);

      for (OResult d : result) {
        Assert.assertNotNull(d.getProperty("location"), "Found null in resultset with groupby");
      }

    } finally {
      database.command("delete vertex GroupByTest").close();
      database.command("drop class GroupByTest UNSAFE").close();
    }
  }
}
