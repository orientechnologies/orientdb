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

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.schedule.OScheduledEventBuilder;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.util.List;

/**
 * Tests the behavior of the schedule rin case of distributed execution.
 */
public class DistributedSchedulerTest extends AbstractServerClusterTest {
  private final static int SERVERS = 2;
  private OFunction        func;

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getDatabaseURL(serverInstance.get(0)));
    db.open("admin", "admin");

    db.getMetadata().getSchema().createClass("scheduler_log");

    func = db.getMetadata().getFunctionLibrary().createFunction("testFunction");
    func.setLanguage("SQL");
    func.setCode("insert into scheduler_log set timestamp = sysdate()");
    func.save();

    db.getMetadata().getScheduler()
        .scheduleEvent(new OScheduledEventBuilder().setName("test").setRule("0/1 * * * * ?").setFunction(func).build());

    Thread.sleep(5000);

    List<ODocument> result = (List<ODocument>) db.command(new OCommandSQL("select count(*) from scheduler_log")).execute();
    Long count = result.get(0).field("count");

    Assert.assertTrue(count >= 4 && count <= 5);

    db.close();
  }

  @Override
  protected String getDatabaseURL(ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  protected String getDatabaseName() {
    return "testDistributedScheduler";
  }
}
