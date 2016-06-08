/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OScheduledEventBuilder;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests the behavior of the schedule rin case of distributed execution.
 */
public class DistributedSchedulerTest extends AbstractServerClusterTest {
  private final static int SERVERS = 2;

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
//    eventByAPI();
    eventBySQL();
  }

  private void eventByAPI() throws InterruptedException {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getDatabaseURL(serverInstance.get(0)));
    db.open("admin", "admin");

    OFunction func = createFunction(db);

    db.getMetadata().getScheduler()
        .scheduleEvent(new OScheduledEventBuilder().setName("test").setRule("0/1 * * * * ?").setFunction(func).build());

    Thread.sleep(5000);

    Long count = getLogCounter(db);

    Assert.assertTrue(count >= 4 && count <= 5);

    db.getMetadata().getScheduler().removeEvent("test");

    db.close();
  }

  public void eventBySQL() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getDatabaseURL(serverInstance.get(0)));
    db.open("admin", "admin");
    try {
      OFunction func = createFunction(db);

      // CREATE NEW EVENT
      db.command(new OCommandSQL("insert into oschedule set name = 'test', function = ?, rule = \"0/1 * * * * ?\""))
          .execute(func.getId());

      Thread.sleep(4000);

      long count = getLogCounter(db);

      Assert.assertTrue(count >= 4);

      db.getLocalCache().invalidate();

      // UPDATE
      db.command(new OCommandSQL("update oschedule set rule = \"0/2 * * * * ?\" where name = 'test'")).execute(func.getId());

      Thread.sleep(4000);

      long newCount = getLogCounter(db);

      Assert.assertTrue(newCount - count > 1 && newCount - count <= 2);

      // DELETE
      db.command(new OCommandSQL("delete from oschedule where name = 'test'")).execute(func.getId());

      Thread.sleep(3000);

      count = newCount;

      newCount = getLogCounter(db);

      Assert.assertTrue(newCount - count <= 1);

    } finally {
      db.drop();
    }
  }

  @Override
  protected String getDatabaseURL(ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  protected String getDatabaseName() {
    return "testDistributedScheduler";
  }

  private OFunction createFunction(final ODatabaseDocumentTx db) {
    if (!db.getMetadata().getSchema().existsClass("scheduler_log"))
      db.getMetadata().getSchema().createClass("scheduler_log");

    OFunction func = db.getMetadata().getFunctionLibrary().getFunction("logEvent");
    if (func == null) {
      func = db.getMetadata().getFunctionLibrary().createFunction("logEvent");
      func.setLanguage("SQL");
      func.setCode("insert into scheduler_log set timestamp = sysdate(), note = :note");
      final List<String> pars = new ArrayList<String>();
      pars.add("note");
      func.setParameters(pars);
      func.save();
    }
    return func;
  }

  private Long getLogCounter(final ODatabaseDocumentTx db) {
    db.activateOnCurrentThread();
    List<ODocument> result = (List<ODocument>) db.command(new OCommandSQL("select count(*) from scheduler_log")).execute();
    return result.get(0).field("count");
  }
}
