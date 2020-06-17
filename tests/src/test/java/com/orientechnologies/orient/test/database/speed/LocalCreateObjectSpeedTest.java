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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;
import com.orientechnologies.orient.test.domain.business.Account;
import java.util.Date;
import org.testng.annotations.Test;

@Test(enabled = false)
public class LocalCreateObjectSpeedTest extends OrientMonoThreadTest {
  private OObjectDatabaseTx database;
  private Account account;
  private Date date = new Date();

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    LocalCreateObjectSpeedTest test = new LocalCreateObjectSpeedTest();
    test.data.go(test);
  }

  public LocalCreateObjectSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  @Override
  public void init() {
    Orient.instance().getProfiler().startRecording();

    database = new OObjectDatabaseTx(System.getProperty("url")).open("admin", "admin");
    database.getEntityManager().registerEntityClass(Account.class);
    database.declareIntent(new OIntentMassiveInsert());
    database.begin(TXTYPE.NOTX);
  }

  @Override
  public void cycle() {
    account = new Account((int) data.getCyclesDone(), "Luca", "Garulli");
    account.setBirthDate(date);
    account.setSalary(3000f + data.getCyclesDone());

    database.save(account);

    if (data.getCyclesDone() == data.getCycles() - 1) database.commit();

    account = null;
  }

  @Override
  public void deinit() {
    database.close();
    super.deinit();
  }
}
