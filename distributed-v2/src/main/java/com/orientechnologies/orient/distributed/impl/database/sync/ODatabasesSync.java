package com.orientechnologies.orient.distributed.impl.database.sync;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import java.util.Map;
import java.util.UUID;

public class ODatabasesSync {
  private Map<UUID, ODatabaseSyncReceiver> runningSyncs;

  public synchronized void startSync(
      OrientDBDistributed orientDBDistributed, String database, UUID uuid, boolean incremental) {
    ODatabaseSyncReceiver receiver =
        new ODatabaseSyncReceiver(orientDBDistributed, database, incremental);
    receiver.run();
    runningSyncs.put(uuid, receiver);
  }

  public synchronized void startChunk(UUID uuid, byte[] bytes, int len) {
    ODatabaseSyncReceiver sync = runningSyncs.get(uuid);
    sync.receive(bytes, len);
  }
}
