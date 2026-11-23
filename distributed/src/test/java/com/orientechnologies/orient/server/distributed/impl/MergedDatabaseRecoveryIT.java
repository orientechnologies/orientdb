/*
 * Copyright 2026 OrientDB LTD
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
package com.orientechnologies.orient.server.distributed.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.hazelcast.core.LifecycleEvent;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class MergedDatabaseRecoveryIT extends AbstractServerClusterTest {
  private static final String MARKER_CLASS = "MergedDatabaseRecoveryMarker";
  private static final String REJOINED_NODE = "europe-1";

  @Test
  public void recoversExistingDatabaseAfterMergedWithEmptyLocalCache() throws Exception {
    init(2);
    prepare(true);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    final ServerRun primaryServer = serverInstance.get(0);
    final ServerRun rejoinedServer = serverInstance.get(1);
    final ODistributedPlugin primary =
        (ODistributedPlugin) primaryServer.getServerInstance().getDistributedManager();
    final ODistributedPlugin rejoined =
        (ODistributedPlugin) rejoinedServer.getServerInstance().getDistributedManager();

    createMarker(primaryServer);
    assertMarkerAvailable(rejoinedServer);

    final CountDownLatch merged = new CountDownLatch(1);
    final AtomicReference<LifecycleEvent> mergedEvent = new AtomicReference<>();
    final String lifecycleListener =
        rejoined
            .getHazelcastInstance()
            .getLifecycleService()
            .addLifecycleListener(
                event -> {
                  if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                    mergedEvent.set(event);
                    merged.countDown();
                  }
                });

    try {
      final String oldUuid = rejoined.getClusterManager().getLocalNodeUuid();
      rejoinedServer.disconnectFrom(primaryServer);

      waitForDatabaseStatus(
          0,
          REJOINED_NODE,
          getDatabaseName(),
          ODistributedServerManager.DB_STATUS.NOT_AVAILABLE,
          90000);
      waitForDatabaseStatus(
          1,
          primary.getLocalNodeName(),
          getDatabaseName(),
          ODistributedServerManager.DB_STATUS.NOT_AVAILABLE,
          90000);

      rejoinedServer.rejoin(primaryServer);
      assertTrue("Rejoined node did not receive MERGED", merged.await(90, TimeUnit.SECONDS));
      assertFalse(
          "Hazelcast did not replace the UUID during rejoin",
          oldUuid.equals(rejoined.getClusterManager().getLocalNodeUuid()));

      rejoined.setDatabaseStatus(
          REJOINED_NODE, getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);

      rejoined.getClusterManager().stateChanged(mergedEvent.get());

      assertDatabaseAdvertised(
          "local node", rejoined.getLocalNodeConfiguration(), getDatabaseName());
      final ODocument peerNodeConfiguration =
          primary
              .getClusterManager()
              .getNodeConfigurationByUuid(rejoined.getClusterManager().getLocalNodeUuid(), false);
      assertDatabaseAdvertised("primary node", peerNodeConfiguration, getDatabaseName());

      new OClusterHealthChecker(rejoined, 0).run();

      assertEquals(
          ODistributedServerManager.DB_STATUS.ONLINE,
          rejoined.getDatabaseStatus(REJOINED_NODE, getDatabaseName()));
      waitForDatabaseStatus(
          0, REJOINED_NODE, getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE, 10000);
      assertMarkerAvailable(rejoinedServer);
    } finally {
      rejoined
          .getHazelcastInstance()
          .getLifecycleService()
          .removeLifecycleListener(lifecycleListener);
    }
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "proxied-orientdb-dserver-config-" + server.getServerId() + ".xml";
  }

  @Override
  public String getDatabaseName() {
    return "merged-database-recovery";
  }

  private void createMarker(final ServerRun server) {
    try (ODatabaseDocumentInternal database = getDatabase(server)) {
      database.command(new OCommandSQL("create class " + MARKER_CLASS)).execute();
      database.save(new ODocument(MARKER_CLASS).field("marker", "before-rejoin"));
    }
  }

  private void assertMarkerAvailable(final ServerRun server) {
    try (ODatabaseDocumentInternal database = getDatabase(server)) {
      assertEquals(1, database.countClass(MARKER_CLASS));
    }
  }

  private static void assertDatabaseAdvertised(
      final String observer, final ODocument configuration, final String databaseName) {
    assertNotNull(configuration);
    final Collection<?> databases = configuration.field("databases");
    assertNotNull(observer + " did not publish databases", databases);
    assertTrue(observer + " did not advertise " + databaseName, databases.contains(databaseName));
  }
}
