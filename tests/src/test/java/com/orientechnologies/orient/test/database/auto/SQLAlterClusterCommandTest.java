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

import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-cluster-alter")
public class SQLAlterClusterCommandTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLAlterClusterCommandTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testCreateCluster() {
    int expectedClusters = database.getClusters();
    try {
      database.command("create cluster europe");
      Assert.assertEquals(database.getClusters(), expectedClusters + 1);
    } finally {
      database.command("drop cluster europe");
    }
    Assert.assertEquals(database.getClusters(), expectedClusters);
  }

  @Test
  public void testAlterClusterName() {
    try {
      database.command("create cluster europe");
      database.command("ALTER CLUSTER europe NAME \"my_orient\"");

      int clusterId = database.getClusterIdByName("my_orient");
      Assert.assertEquals(clusterId, 18);
    } finally {
      database.command("drop cluster my_orient");
    }
  }
}
