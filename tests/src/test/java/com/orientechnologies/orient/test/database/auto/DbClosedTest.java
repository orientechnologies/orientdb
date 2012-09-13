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

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.object.db.OObjectDatabasePool;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.test.database.base.SetupTest;

@Test(groups = "db")
public class DbClosedTest {
  final private String url;

  @Parameters(value = { "url" })
  public DbClosedTest(final String iURL) {
    url = iURL;
  }

  public void testStorageClosed() {
    if (SetupTest.instance().isReuseDatabase())
      return;

    ODatabaseDocumentPool.global().close();
    OObjectDatabasePool.global().close();

    if (OGlobalConfiguration.STORAGE_KEEP_OPEN.getValueAsBoolean())
      return;

    // for (OStorage stg : Orient.instance().getStorages()) {
    // Assert.assertTrue(stg.isClosed());
    // }
  }

  public void testDoubleDb() {
    OObjectDatabaseTx db = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    // now I am getting another db instance
    OObjectDatabaseTx dbAnother = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    dbAnother.close();

    db.close();
  }

  public void testDoubleDbWindowsPath() {
    OObjectDatabaseTx db = OObjectDatabasePool.global().acquire(url.replace('/', '\\'), "admin", "admin");

    // now I am getting another db instance
    OObjectDatabaseTx dbAnother = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    dbAnother.close();

    db.close();
  }
}
