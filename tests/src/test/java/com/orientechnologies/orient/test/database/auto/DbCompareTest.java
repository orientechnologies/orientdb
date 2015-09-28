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

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups = "db")
public class DbCompareTest extends DocumentDBBaseTest implements OCommandOutputListener {
  final private String testPath;

  @Parameters(value = { "url", "testPath" })
  public DbCompareTest(@Optional String url, String testPath) {
    super(url);
    this.testPath = testPath;
  }

  @Test
  public void testCompareDatabases() throws IOException {
    String urlPrefix = getStorageType() + ":";

    final ODatabaseCompare databaseCompare = new ODatabaseCompare(url, urlPrefix + testPath + "/" + DbImportExportTest.NEW_DB_URL,
        "admin", "admin", this);
    databaseCompare.setCompareEntriesForAutomaticIndexes(true);
    databaseCompare.setCompareIndexMetadata(true);
    Assert.assertTrue(databaseCompare.compare());
  }

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
  }
}
