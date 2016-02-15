/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.orient.server.distributed.scenariotest;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.*;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server (quorum=1)
 * - server3 is isolated
 * - insert on server3 succeeds
 * - server3 joins the cluster
 * - two writes on server3:
 *      - the first one fails due to rid disalignment
 *      - the second one succeeds after the fix task (wait for the alignment before the seconde write)
 */

public class RIDHotAlignmentScenarioTest extends AbstractScenarioTest {

    @Ignore
    @Test
    public void test() throws Exception {

        maxRetries = 10;
        init(SERVERS);
        prepare(false);

        // execute writes only on server3
        executeWritesOnServers.add(serverInstance.get(2));

        execute();
    }

    @Override
    public void executeTest() throws Exception {

        /*
             * Test with quorum = 1
             */

        banner("Test with quorum = 1");

        // changing configuration: writeQuorum=1, autoDeploy=false, hotAlignment=true
        System.out.print("\nChanging configuration (writeQuorum=1, autoDeploy=false, hotAlignment=true)...");

        ODocument cfg = null;
        ServerRun server = serverInstance.get(2);
        OHazelcastPlugin manager = (OHazelcastPlugin) server.getServerInstance().getDistributedManager();
        ODistributedConfiguration databaseConfiguration = manager.getDatabaseConfiguration("distributed-inserttxha");
        cfg = databaseConfiguration.serialize();
        cfg.field("writeQuorum", 1);
        cfg.field("failureAvailableNodesLessQuorum", true);
        cfg.field("autoDeploy", true);
        cfg.field("hotAlignment", true);
        cfg.field("version", (Integer) cfg.field("version") + 1);
        manager.updateCachedDatabaseConfiguration("distributed-inserttxha", cfg, true, true);
        System.out.println("\nConfiguration updated.");

        // creating class "Hero"
        ODatabaseDocumentTx dbServer1 = poolFactory.get(getPlocalDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
        ODatabaseDocumentTx dbServer2 = poolFactory.get(getPlocalDatabaseURL(serverInstance.get(1)), "admin", "admin").acquire();
        ODatabaseDocumentTx dbServer3 = new ODatabaseDocumentTx(getPlocalDatabaseURL(serverInstance.get(2))).open("admin", "admin");
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
        dbServer1.getMetadata().getSchema().createClass("Hero");

        // checking the cluster "Hero" is present on each server
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
        HashSet<String> clustersOnServer1 = (HashSet<String>) dbServer1.getClusterNames();
        assertTrue(clustersOnServer1.contains("hero"));
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer2);
        HashSet<String> clustersOnServer2 = (HashSet<String>) dbServer2.getClusterNames();
        assertTrue(clustersOnServer2.contains("hero"));
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
        HashSet<String> clustersOnServer3 = (HashSet<String>) dbServer3.getClusterNames();
        assertTrue(clustersOnServer3.contains("hero"));

        String clusterName34 = dbServer3.getClusterNameById(34);
        String clusterName36 = dbServer3.getClusterNameById(35);

        // isolating server3
        System.out.println("Network fault on server3.\n");
        simulateServerFault(serverInstance.get(2), "net-fault");
        assertFalse(serverInstance.get(2).isActive());

        // first insert with server3 isolated form the cluster
        banner("First insert on server 3 (joining isolated from the the cluster)");
        ODocument firstInsert = null;
        ORID rid1 = null;
        try {
            dbServer3 = new ODatabaseDocumentTx(getPlocalDatabaseURL(serverInstance.get(2))).open("admin","admin");
            firstInsert = new ODocument("Hero").fields("name", "Luke", "surname", "Skywalker").save();
            System.out.println(firstInsert.getRecord().toString());
            rid1 = firstInsert.getRecord().getIdentity();
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }

        // the record was inserted on server2
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
        long recordCount = dbServer3.countClass("Hero");
        assertEquals(1, recordCount);

        // checking the record was not inserted on server1 and server2 (checking the record amount)
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
        recordCount = dbServer1.countClass("Hero");
        assertEquals(0, recordCount);
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer2);
        recordCount = dbServer2.countClass("Hero");
        assertEquals(0, recordCount);

        // server3 joins the cluster
        try {
            serverInstance.get(2).startServer(getDistributedServerConfiguration(server));
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }

        Thread.sleep(500);

        // second insert with server3 joining the cluster that must fail
        banner("Second insert on server1 (server3 joining the cluster)");
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
        ODocument secondInsert = null;
        ORID rid2 = null;
        try {
            dbServer1 = poolFactory.get(getPlocalDatabaseURL(serverInstance.get(1)), "admin", "admin").acquire();
            secondInsert = new ODocument("Hero").fields("name", "Han", "surname", "Solo").save();
            System.out.println(secondInsert.getRecord().toString());
            rid2 = secondInsert.getRecord().getIdentity();
            System.out.println("Insert succeeded.");
            //            assertTrue(false);
        } catch (Exception e) {
            System.out.println("Insert failed.");
            e.printStackTrace();
            assertTrue(true);
        }


        // the record wasn't inserted
        recordCount = dbServer1.countClass("Hero");
        //        assertEquals(1, recordCount);

        // waiting for the hotAlignment
        Thread.sleep(500);

        // third insert with server3 joining the cluster that must succeed
        banner("Third insert on server1 (server3 joining the cluster)");
        ODocument thirdInsert = null;
        ORID rid3 = null;
        try {
            dbServer1 = new ODatabaseDocumentTx(getPlocalDatabaseURL(serverInstance.get(1))).open("admin", "admin");
            thirdInsert = new ODocument("Hero").fields("name", "Han", "surname", "Solo").save();
            System.out.println(thirdInsert.getRecord().toString());
            rid3 = thirdInsert.getRecord().getIdentity();
            //            assertTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(true);
        }

        // the record was inserted
        recordCount = dbServer1.countClass("Hero");
        //        assertEquals(2, recordCount);

        // check consistency above the cluster

    }
}
