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

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "db")
public class DbCopyTest extends DocumentDBBaseTest implements OCommandOutputListener {

  @Parameters(value = {"url"})
  public DbCopyTest(@Optional String url) {
    super(url);
  }

  @Test
  public void checkCopy() throws IOException {
    final String className = "DbCopyTest";
    database.getMetadata().getSchema().createClass(className);

    Thread thread =
        new Thread() {
          @Override
          public void run() {
            final ODatabaseDocument otherDB = database.copy();
            otherDB.activateOnCurrentThread();
            for (int i = 0; i < 5; i++) {
              ODocument doc = otherDB.newInstance(className);
              doc.field("num", i);
              doc.save();
              try {
                Thread.sleep(10);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
            otherDB.close();
          }
        };
    thread.start();

    for (int i = 0; i < 20; i++) {
      ODocument doc = database.newInstance(className);
      doc.field("num", i);
      doc.save();
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    try {
      thread.join();
    } catch (InterruptedException e) {
      Assert.fail();
    }

    List<ODocument> result =
        database.query(new OSQLSynchQuery<ODocument>("SELECT FROM " + className));

    Assert.assertEquals(result.size(), 25);
  }

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
    // System.out.print(iText);
    // System.out.flush();
  }
}
