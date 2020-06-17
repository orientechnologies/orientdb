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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.test.database.base.SetupTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "db")
public class DbClosedTest extends DocumentDBBaseTest {
  private OPartitionedDatabasePool pool;

  @Parameters(value = {"url"})
  public DbClosedTest(@Optional String url) {
    super(url);
    setAutoManageDatabase(false);
    setDropDb(true);
  }

  @BeforeClass
  public void before() {
    pool = new OPartitionedDatabasePool(url, "admin", "admin");
  }

  @AfterClass
  public void after() {
    pool.close();
  }

  public void testDoubleDb() {
    ODatabaseDocumentTx db = pool.acquire();

    // now I am getting another db instance
    ODatabaseDocumentTx dbAnother = pool.acquire();
    dbAnother.close();

    db.activateOnCurrentThread();
    db.close();
  }

  public void testDoubleDbWindowsPath() {
    ODatabaseDocumentTx db = pool.acquire();

    // now I am getting another db instance
    ODatabaseDocumentTx dbAnother = pool.acquire();
    dbAnother.close();

    db.activateOnCurrentThread();
    db.close();
  }

  @Test(dependsOnMethods = {"testDoubleDb", "testDoubleDbWindowsPath"})
  public void testStorageClosed() {
    if (SetupTest.instance().isReuseDatabase()) return;
  }

  @Test
  public void testRemoteConns() {
    if (!url.startsWith("remote:")) return;

    final int max = OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.getValueAsInteger();
    for (int i = 0; i < max * 2; ++i) {
      final ODatabase db = new ODatabaseDocumentTx(url).open("admin", "admin");
      db.close();
    }
  }
}
