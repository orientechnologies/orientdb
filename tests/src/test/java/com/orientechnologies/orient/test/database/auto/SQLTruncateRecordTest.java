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
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups = "sql-delete")
public class SQLTruncateRecordTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLTruncateRecordTest(@Optional String url) {
    super(url);
  }

  @Test
  public void truncateRecord() {
    if (!database.getMetadata().getSchema().existsClass("Person"))
      database.command(new OCommandSQL("create class Profile")).execute();

    database.command(new OCommandSQL("insert into Profile (sex, salary) values ('female', 2100)")).execute();

    final Long total = database.countClass("Profile");

    final List<ODocument> resultset = database
        .query(new OSQLSynchQuery<Object>("select from Profile where sex = 'female' and salary = 2100"));

    final Number records = (Number) database.command(new OCommandSQL("truncate record [" + resultset.get(0).getIdentity() + "]"))
        .execute();

    Assert.assertEquals(records.intValue(), 1);

    Assert.assertEquals(database.countClass("Profile"), total - records.intValue());
  }

  @Test
  public void truncateNonExistingRecord() {
    if (!database.getMetadata().getSchema().existsClass("Person"))
      database.command(new OCommandSQL("create class Profile")).execute();

    final Number records = (Number) database
        .command(new OCommandSQL("truncate record [ #" + database.getClusterIdByName("Profile") + ":99999999 ]")).execute();

    Assert.assertEquals(records.intValue(), 0);
  }
}
