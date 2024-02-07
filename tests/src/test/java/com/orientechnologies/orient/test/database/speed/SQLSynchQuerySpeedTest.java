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
package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.test.database.base.OrientTest;
import java.io.UnsupportedEncodingException;
import org.testng.annotations.Test;

@Test(enabled = false)
public class SQLSynchQuerySpeedTest extends SpeedTestMonoThread implements OCommandResultListener {
  protected int resultCount = 0;
  private ODatabaseDocument database;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    SQLSynchQuerySpeedTest test = new SQLSynchQuerySpeedTest();
    test.data.go(test);
  }

  public SQLSynchQuerySpeedTest() {
    super(1);
    database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");

    System.out.println("Finding Accounts between " + database.countClass("Profile") + " records");
  }

  @Override
  public void cycle() throws UnsupportedEncodingException {
    OResultSet result = database.query("select * from Profile where nick = 100010");

    while (result.hasNext()) result(result.next().getElement().get());
  }

  public boolean result(final Object iRecord) {
    OrientTest.printRecord(resultCount++, iRecord);
    return true;
  }

  @Override
  public void end() {}

  @Override
  public Object getResult() {
    return null;
  }
}
