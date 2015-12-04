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
package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;
import org.testng.annotations.Test;

import java.util.Date;

@Test(enabled = false)
public class BatchSpeedTest extends OrientMonoThreadTest {
  private ODatabaseDocument database;
  private Date              date  = new Date();
  private final static long DELAY = 0;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    BatchSpeedTest test = new BatchSpeedTest();
    test.data.go(test);
  }

  public BatchSpeedTest() throws InstantiationException, IllegalAccessException {
    super(10000);
  }

  @Override
  public void init() {
    Orient.instance().getProfiler().startRecording();
    database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");
  }

  @Override
  public void cycle() {
    database
        .command(new OCommandScript("sql",
            "BEGIN\n" + "let v1 = select from V limit 100\n"
                + "let v2 = create vertex v set when = date()\n" + "create edge from $v1 to $v2\n" + "COMMIT RETRY 100\n"))
        .execute();

    if (DELAY > 0)
      try {
        Thread.sleep(DELAY);
      } catch (InterruptedException e) {
      }
  }

  @Override
  public void deinit() {
    System.out.println(Orient.instance().getProfiler().dump());

    if (database != null)
      database.close();
    super.deinit();
  }
}
