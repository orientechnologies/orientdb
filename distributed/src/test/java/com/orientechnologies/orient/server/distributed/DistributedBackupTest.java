/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.distributed;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Distributed BACKUP test against "plocal" protocol.
 */
public class DistributedBackupTest extends AbstractServerClusterTest {
  final static int SERVERS  = 3;
  final static int VERTICES = 1000;

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    OrientGraphFactory localFactory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
    OrientGraphNoTx graphNoTx = localFactory.getNoTx();

    try {
      final long initialVertices = graphNoTx.countVertices();

      banner("START BACKUP ON SERVER 0...");

      final AtomicBoolean finished = new AtomicBoolean(false);

      final OutputStream out = new FileOutputStream("backup.zip");
      localFactory.getDatabase().backup(out, null, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          new Thread(new Runnable() {
            @Override
            public void run() {
              banner("DURING BACKUP CREATE VERTICES...");

              insertVertices();

              finished.set(true);
            }
          }).start();

          while (!finished.get())
            Thread.sleep(1000);

          return null;
        }
      }, null, 9, 128000);

      banner("BACKUP FINISHED, WAIT 5 SECS FOR THE REALIGNMENT...");

      Thread.sleep(5000);

      banner("CHECKING IF ALL VERTICES ARE PROPAGATED TO THE NODE 0 THAT RUN THE BACKUP");

      graphNoTx.makeActive();
      Assert.assertEquals(graphNoTx.countVertices() - initialVertices, VERTICES);

    } finally {
      graphNoTx.shutdown();
    }
  }

  protected void insertVertices() {
    OrientGraphFactory localFactory = new OrientGraphFactory("plocal:target/server1/databases/" + getDatabaseName());
    OrientGraphNoTx graphNoTx = localFactory.getNoTx();
    try {
      for (int i = 0; i < VERTICES; ++i) {
        graphNoTx.addVertex(null);

        if (i % 100 == 0) {
          System.out.println("Created " + i + " vertices");
        }

      }
    } finally {
      graphNoTx.shutdown();
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-backup";
  }

}
