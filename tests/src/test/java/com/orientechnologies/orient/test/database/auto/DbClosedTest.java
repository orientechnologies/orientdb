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

import com.orientechnologies.orient.core.config.OConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "db")
public class DbClosedTest extends DocumentDBBaseTest {

  @Parameters(value = {"url"})
  public DbClosedTest(@Optional String url) {
    super(url);
    setAutoManageDatabase(false);
    setDropDb(true);
  }

  @Test
  public void testRemoteConns() {
    if (!url.startsWith("remote:")) return;

    final int max = OConfiguration.global().networkMaxConcurrentSessions();
    for (int i = 0; i < max * 2; ++i) {
      final ODatabase db = openSession("admin", "admin");
      db.close();
    }
  }
}
