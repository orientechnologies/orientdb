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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Test(groups = "sql-select")
public class SQLSelectGroupByTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLSelectGroupByTest(@Optional String url) {
    super(url);
  }

  @Test
  public void queryGroupByBasic() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select location from Account group by location"))
        .execute();

    Assert.assertTrue(result.size() > 1);
    Set<Object> set = new HashSet<Object>();
    for (ODocument d : result) {
        set.add(d.field("location"));
    }
    Assert.assertEquals(result.size(), set.size());
  }

  @Test
  public void queryGroupByLimit() {
    List<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select location from Account group by location limit 2")).execute();

    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void queryGroupByCount() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select count(*) from Account group by location"))
        .execute();

    Assert.assertTrue(result.size() > 1);
  }

  @Test
  public void queryGroupByAndOrderBy() {
    List<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select location from Account group by location order by location")).execute();

    Assert.assertTrue(result.size() > 1);
    String last = null;
    for (ODocument d : result) {
      if (last != null) {
          Assert.assertTrue(last.compareTo((String) d.field("location")) < 0);
      }
      last = d.field("location");
    }

    result = database.command(
        new OSQLSynchQuery<ODocument>("select location from Account group by location order by location desc")).execute();

    Assert.assertTrue(result.size() > 1);
    last = null;
    for (ODocument d : result) {
      if (last != null) {
          Assert.assertTrue(last.compareTo((String) d.field("location")) > 0);
      }
      last = d.field("location");
    }
  }
}
