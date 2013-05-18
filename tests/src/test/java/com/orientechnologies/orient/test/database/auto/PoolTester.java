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

import java.util.List;
import java.util.logging.Logger;

import org.testng.annotations.Test;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
public class PoolTester {
  static Logger logger = Logger.getLogger(PoolTester.class.getName());

  public OGraphDatabase getConnection() {
    String url = "remote:localhost/demo";
    String user = "admin";
    String password = "admin";
    return OGraphDatabasePool.global().acquire(url, user, password);
  }

  public static void main(String[] args) throws InterruptedException {
    PoolTester pt = new PoolTester();
    int runs = 20;

    logger.info("start");

    for (int i = 0; i < runs; i++) {
      pt.doStuff(i);
    }
    logger.info("***** sleeping for 30 sec, stop and start Orientdb now! *****");
    Thread.sleep(30 * 1000);
    for (int i = 0; i < runs; i++) {
      pt.doStuff(i);
    }
    logger.info("done");
  }

  private void doStuff(int n) {
    OGraphDatabase gdb = getConnection();
    logger.info("#" + n + " - " + gdb.hashCode());
    try {
      OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM V");
      List<ODocument> result = gdb.command(query).execute();
      if (result.size() > 0) {
        ODocument doc = result.get(0);
      }
    } catch (OException e) {
      System.err.println("Query failed");
    } finally {
      gdb.close();
    }
  }
}