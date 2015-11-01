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

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Issue #5081 (https://github.com/orientechnologies/orientdb/issues/5081)
 */
@Test
public class SQLScanTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLScanTest(@Optional String url) {
    super(url);
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    if (skipTestIfRemote())
      return;

    for (int i = 0; i < 10000; ++i) {
      new ODocument("TestScan").field("id", i).field("age", i % 10).save();
    }
  }

  public void scanClusterAscendingOrder() throws IOException {
    if (skipTestIfRemote())
      return;

    final OStorage stg = database.getStorage().getUnderlying();

    final AtomicLong found = new AtomicLong();

    ((OAbstractPaginatedStorage) stg).scanCluster("TestScan", true, -1, -1, new OCallable<Boolean, ORecord>() {
      @Override
      public Boolean call(ORecord iArgument) {
        found.incrementAndGet();
        return true;
      }
    });

    Assert.assertEquals(found.get(), 10000);
  }

  public void scanClusterDescendingOrder() throws IOException {
    if (skipTestIfRemote())
      return;

    final OStorage stg = database.getStorage().getUnderlying();

    final AtomicLong found = new AtomicLong();

    ((OAbstractPaginatedStorage) stg).scanCluster("TestScan", false, -1, -1, new OCallable<Boolean, ORecord>() {
      @Override
      public Boolean call(ORecord iArgument) {
        found.incrementAndGet();
        return true;
      }
    });

    Assert.assertEquals(found.get(), 10000);
  }

  public void scanClusterLimited() throws IOException {
    if (skipTestIfRemote())
      return;

    final OStorage stg = database.getStorage().getUnderlying();

    final AtomicLong found = new AtomicLong();

    ((OAbstractPaginatedStorage) stg).scanCluster("TestScan", false, 0, 9, new OCallable<Boolean, ORecord>() {
      @Override
      public Boolean call(ORecord iArgument) {
        found.incrementAndGet();
        return true;
      }
    });

    Assert.assertEquals(found.get(), 10);
  }

  public void scanClusterRespectSkip() throws IOException {
    if (skipTestIfRemote())
      return;

    final OStorage stg = database.getStorage().getUnderlying();

    final AtomicLong found = new AtomicLong();

    ((OAbstractPaginatedStorage) stg).scanCluster("TestScan", false, -1, -1, new OCallable<Boolean, ORecord>() {
      @Override
      public Boolean call(ORecord iArgument) {
        return found.incrementAndGet() < 10;
      }
    });

    Assert.assertEquals(found.get(), 10);
  }
}
