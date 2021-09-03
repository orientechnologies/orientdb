package com.orientechnologies.orient.distributed.impl.database.sync;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.database.operations.ODatabaseFullSyncChunk;
import com.orientechnologies.orient.distributed.impl.database.operations.ODatabaseFullSyncStart;
import com.orientechnologies.orient.distributed.network.ODistributedNetwork;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

public class ODatabaseFullSyncSender {
  private final OrientDBDistributed databases;
  private final ONodeIdentity requester;
  private final String database;

  public ODatabaseFullSyncSender(
      OrientDBDistributed databases, ONodeIdentity requester, String database) {
    this.databases = databases;
    this.requester = requester;
    this.database = database;
  }

  public void run() {
    new Thread(
            () -> {
              ODistributedNetwork network = databases.getNetworkManager();
              ODatabaseDocumentEmbedded session = databases.openNoAuthorization(database);
              boolean incremental = session.getStorage().supportIncremental();
              UUID uuid = UUID.randomUUID();
              network.send(requester, new ODatabaseFullSyncStart(database, uuid, incremental));
              OutputStream outputStream =
                  new BufferedOutputStream(
                      new OutputStream() {
                        @Override
                        public void write(byte[] b, int off, int len) throws IOException {
                          network.send(
                              requester, new ODatabaseFullSyncChunk(database, uuid, b, len));
                        }

                        @Override
                        public void write(int b) throws IOException {
                          throw new UnsupportedOperationException();
                        }
                      });
              if (incremental) {
                session.getStorage().fullIncrementalBackup(outputStream);
              } else {
                try {
                  int compression =
                      session
                          .getConfiguration()
                          .getValueAsInteger(
                              OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION);
                  session.getStorage().backup(outputStream, null, null, null, compression, 1024);
                } catch (IOException e) {
                  // TODO: Handle excpetion here
                  e.printStackTrace();
                }
              }
              session.close();
            })
        .start();
  }
}
