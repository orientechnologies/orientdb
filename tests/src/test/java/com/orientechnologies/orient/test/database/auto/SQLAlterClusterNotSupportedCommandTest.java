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

import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-cluster-alter")
public class SQLAlterClusterNotSupportedCommandTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLAlterClusterNotSupportedCommandTest(@Optional String url) {
    super(url);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testAlterClusterEncryption() {
    try {
      database.command("create cluster europe");
      database.command("ALTER CLUSTER europe encryption aes");
    } finally {
      database.command("drop cluster europe");
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testClusterCompression_lowercase() {
    try {
      database.command("create cluster europe");
      database.command("alter cluster europe compression gzip");
    } finally {
      database.command("drop cluster europe");
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testClusterCompression_uppercase() {
    try {
      database.command("create cluster europe");
      database.command("alter cluster europe COMPRESSION gzip");
    } finally {
      database.command("drop cluster europe");
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testClusterRecordOverflowGrowFactor() {
    try {
      database.command("create cluster europe");
      database.command("alter cluster europe RECORD_OVERFLOW_GROW_FACTOR 3");
    } finally {
      database.command("drop cluster europe");
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testClusterWAL() {
    try {
      database.command("create cluster europe");
      database.command("alter cluster europe USE_WAL true");
    } finally {
      database.command("drop cluster europe");
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testClusterRecordGrowFactor() {
    try {
      database.command("create cluster europe");
      database.command("alter cluster europe RECORD_GROW_FACTOR 3");
    } finally {
      database.command("drop cluster europe");
    }
  }
}
