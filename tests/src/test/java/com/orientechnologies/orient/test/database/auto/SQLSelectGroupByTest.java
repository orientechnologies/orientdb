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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-select")
public class SQLSelectGroupByTest {
  private String            url;
  private ODatabaseDocument database;

  @Parameters(value = "url")
  public SQLSelectGroupByTest(String iURL) {
    url = iURL;
    database = new ODatabaseDocumentTx(iURL);
  }

  @Test
  public void queryGroupByBasic() {
    database.open("admin", "admin");

    try {
      List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select location from Account group by location"))
          .execute();

      Assert.assertTrue(result.size() > 1);
      Set<Object> set = new HashSet<Object>();
      for (ODocument d : result)
        set.add(d.field("location"));
      Assert.assertEquals(result.size(), set.size());

    } finally {
      database.close();
    }
  }

  @Test
  public void queryGroupByCount() {
    database.open("admin", "admin");

    try {
      List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select count(*) from Account group by location"))
          .execute();

      Assert.assertTrue(result.size() > 1);
    } finally {
      database.close();
    }
  }

  @Test
  public void queryGroupByAndOrderBy() {
    database.open("admin", "admin");

    try {
      List<ODocument> result = database.command(
          new OSQLSynchQuery<ODocument>("select location from Account group by location order by location")).execute();

      Assert.assertTrue(result.size() > 1);
      String last = null;
      for (ODocument d : result) {
        if (last != null)
          Assert.assertTrue(last.compareTo((String) d.field("location")) < 0);
        last = d.field("location");
      }

      result = database.command(
          new OSQLSynchQuery<ODocument>("select location from Account group by location order by location desc")).execute();

      Assert.assertTrue(result.size() > 1);
      last = null;
      for (ODocument d : result) {
        if (last != null)
          Assert.assertTrue(last.compareTo((String) d.field("location")) > 0);
        last = d.field("location");
      }

      database.close();
    } finally {
      database.close();
    }
  }
}
