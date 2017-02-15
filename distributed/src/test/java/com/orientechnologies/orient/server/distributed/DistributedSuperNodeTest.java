/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *  
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Distributed TX test against "plocal" protocol.
 */
public class DistributedSuperNodeTest extends AbstractServerClusterGraphTest {
  @Test
  public void test() throws Exception {
    final long timeout = OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.getValueAsLong();
    OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.setValue(1);
    try {

      count = 200;
      init(3);
      prepare(false);
      execute();

    } finally {
      OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.setValue(timeout);
    }
  }

  @Override
  protected void setFactorySettings(ODatabasePool pool) {
//    pool.setConnectionStrategy(OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_REQUEST.toString());
  }

  @Override
  protected void onAfterExecution() {
    ODatabaseDocument graph = pool.acquire();
    try {
      OVertex root = graph.load(rootVertexId);
      Assert.assertEquals(((OMultiCollectionIterator) root.getEdges(ODirection.OUT)).size(),
          count * serverInstance.size() * writerCount);
    } finally {
      graph.close();
    }
    super.onAfterExecution();
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-graph";
  }
}
