package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.ODatabaseMissing;
import com.orientechnologies.orient.distributed.context.coordination.result.ODatabaseNameUsed;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class ODatabasesTopologyState {

  private final Map<ODatabaseId, ODatabaseTopologyState> databases = new HashMap<>();
  private final Map<String, ODatabaseTopologyState> databasesByName = new HashMap<>();
  private final Map<ODatabaseId, ORawPair<OTransactionIdPromise, ODatabaseTopologyState>> promised =
      new HashMap<>();
  private final Map<String, ODatabaseTopologyState> promisedByName = new HashMap<>();

  public ODatabasesTopologyState() {}

  public synchronized Optional<OAcceptResult> promiseDeclare(
      OTransactionIdPromise promise,
      ODatabaseId db,
      String name,
      Set<ONodeId> partecipants,
      int minimumQuorum) {

    if (databases.containsKey(db)) {
      var def = databases.get(db);
      if (!def.getName().equals(name)) {
        return Optional.empty();
        // Exactly the same
      }
      // TODO: database id used probably different error with retry ....
      return Optional.of(new ODatabaseNameUsed());
    } else {
      if (databasesByName.containsKey(name)) {
        return Optional.of(new ODatabaseNameUsed());
      } else {
        if (promised.containsKey(db)) {
          var prom = promised.get(db);
          var def = prom.getSecond();
          if (!def.getName().equals(name)
              && prom.getFirst().getCoordinator().equals(promise.getCoordinator())) {
            return Optional.empty();
          } else {
            return Optional.of(new OAlreadyPromised());
          }
        }
        if (promisedByName.containsKey(name)) {
          return Optional.of(new OAlreadyPromised());
        }
        var declared = new ODatabaseTopologyState(db, name, partecipants, minimumQuorum);
        this.promised.put(
            db, new ORawPair<OTransactionIdPromise, ODatabaseTopologyState>(promise, declared));
        this.promisedByName.put(name, declared);
        return Optional.empty();
      }
    }
  }

  public synchronized void cancelPomise(
      OTransactionIdPromise promise, ODatabaseId db, String name) {
    if (promised.containsKey(db)) {
      var prom = promised.remove(db);
      var inst = prom.getSecond();
      promisedByName.remove(name);
      assert inst.getName().equals(name);
    }
  }

  public synchronized void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId db,
      String name,
      Set<ONodeId> partecipants,
      int minimumQuorum) {
    if (promised.containsKey(db)) {
      var prom = promised.remove(db);
      var inst = prom.getSecond();
      promisedByName.remove(name);
      assert inst.getName().equals(name);
      this.databases.put(db, inst);
      this.databasesByName.put(inst.getName(), inst);
    } else {
      var declared = new ODatabaseTopologyState(db, name, partecipants, minimumQuorum);
      this.databases.put(db, declared);
      this.databasesByName.put(declared.getName(), declared);
    }
  }

  public synchronized void declareNode(
      ODatabaseId db, String name, ONodeId node, ONodeRole role, ODatabaseState state) {
    var nodes =
        databases.computeIfAbsent(
            db,
            (dbKey) -> {
              return new ODatabaseTopologyState(db, name, Set.of(node), 0);
            });

    // First declare, version 0
    nodes.defineNode(node, role, state, 0);
  }

  public synchronized void setState(
      ODatabaseId db, ONodeId node, ODatabaseState state, long version) {
    var nodes = databases.get(db);
    if (nodes != null) {
      nodes.setState(node, state, version);
    }
  }

  public synchronized Set<ODatabaseId> listDatabaseIds() {
    return this.databases.keySet();
  }

  public synchronized Optional<OAcceptResult> promiseState(
      ODatabaseId dbId, ONodeId nodeId, ODatabaseState state, long version) {
    ODatabaseTopologyState dbTopology = this.databases.get(dbId);
    if (dbTopology != null) {
      return dbTopology.promiseState(state, nodeId, version);
    } else {
      return Optional.of(new ODatabaseMissing());
    }
  }

  public synchronized long getDatabaseVersion(ODatabaseId dbId) {
    ODatabaseTopologyState dbTopology = this.databases.get(dbId);
    if (dbTopology != null) {
      return dbTopology.getVersion();
    } else {
      return 0;
    }
  }

  public synchronized ODatabaseState getNodeState(ODatabaseId dbId, ONodeId nodeId) {
    return this.databases.get(dbId).getState(nodeId);
  }

  public synchronized void cancelPomiseSetState(ODatabaseId dbId, ONodeId nodeId, long version) {
    this.databases.get(dbId).cancelSetState(nodeId, version);
  }

  public synchronized boolean waitOnline(ODatabaseId dbId, ONodeId nodeId) {
    return false;
  }

  public synchronized boolean waitOnlineQuorum(ODatabaseId dbId, Optional<Long> timeout)
      throws InterruptedException {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      return db.waitOnlineQuorum(timeout);
    }
    return false;
  }

  public synchronized boolean waitOnlineOne(ODatabaseId dbId) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      return db.waitOnlineOne();
    }
    return false;
  }

  public synchronized boolean executeOnOneOnline(ODatabaseId dbId, OStateAction execute) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      db.executeOnOneOnline(execute);
      return true;
    }
    return false;
  }

  public synchronized Set<ONodeId> getOnlineNodes(ODatabaseId dbId) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      return db.getOnlineNodes();
    }
    return Collections.emptySet();
  }

  public synchronized OSyncInfo newSync(ODatabaseId dbId) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db == null) {
      throw new NullPointerException("missing database definition");
    }
    return db.newSync();
  }

  public synchronized Optional<OSyncState> canSync(
      ONodeId from, ONodeId to, ODatabaseId dbId, UUID syncId, boolean canSync, OSyncMode mode) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db == null) {
      throw new NullPointerException("missing database definition");
    }
    return db.canSync(from, to, syncId, canSync, mode);
  }

  public OSyncState startSend(
      ONodeId to, ONodeId from, ODatabaseId dbId, UUID syncId, OSyncMode mode) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db == null) {
      throw new NullPointerException("missing database definition");
    }
    return db.startSend(from, to, syncId, mode);
  }
}
