package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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

/** @author Enrico Risa */
public class HaRemoveServerIT extends AbstractServerClusterTest {
  private static final int SERVERS = 2;

  ExecutorService executorService = Executors.newSingleThreadExecutor();

  public String getDatabaseName() {
    return "HaRemoveServerIT";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() {

    ServerRun firstServer = serverInstance.get(0);

    final String offlineNodeName =
        serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName();

    ODistributedServerManager distributedManager =
        firstServer.getServerInstance().getDistributedManager();

    final AtomicReference<ODistributedServerManager.DB_STATUS> ref =
        new AtomicReference<ODistributedServerManager.DB_STATUS>(
            ODistributedServerManager.DB_STATUS.OFFLINE);
    try {

      // 3 events for failing test (NOT_AVAILABLE,SYNCHRONIZING,ONLINE)
      final CountDownLatch latch = new CountDownLatch(3);
      distributedManager.registerLifecycleListener(
          new ODistributedLifecycleListener() {
            @Override
            public boolean onNodeJoining(String iNode) {
              return false;
            }

            @Override
            public void onNodeJoined(String iNode) {}

            @Override
            public void onNodeLeft(String iNode) {}

            @Override
            public void onDatabaseChangeStatus(
                String iNode,
                String iDatabaseName,
                ODistributedServerManager.DB_STATUS iNewStatus) {

              if (iNode.equals(offlineNodeName)) {
                ref.set(iNewStatus);
                latch.countDown();
              }
            }
          });
      Future<Void> voidFuture = invokeRemoveServer(offlineNodeName, serverInstance.get(1));

      voidFuture.get();

      latch.await(10000, TimeUnit.MILLISECONDS);

      Assert.assertEquals(ODistributedServerManager.DB_STATUS.OFFLINE, ref.get());
    } catch (InterruptedException e) {
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.fail();

    } finally {

    }
  }

  protected Future<Void> invokeRemoveServer(final String offlineNodeName, final ServerRun server) {

    Future<Void> future =
        executorService.submit(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {

                try (ODatabaseDocument db =
                    server
                        .getServerInstance()
                        .getContext()
                        .open(getDatabaseName(), "admin", "admin")) {
                  db.command(
                          new OCommandSQL(String.format("HA remove server `%s`", offlineNodeName)))
                      .execute();
                } catch (Exception e) {
                  e.printStackTrace();
                }

                return null;
              }
            });
    return future;
  }

  @Override
  protected void onAfterDatabaseCreation(ODatabaseDocument db) {
    db.command(new OCommandSQL("CREATE CLASS Person extends V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Person.name STRING")).execute();
  }
}
