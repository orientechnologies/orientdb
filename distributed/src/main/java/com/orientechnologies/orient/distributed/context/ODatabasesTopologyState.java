package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.message.ODatabaseStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.ODatabaseMissing;
import com.orientechnologies.orient.distributed.context.coordination.result.ODatabaseNameUsed;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ODatabasesTopologyState implements ODatabasesTopology {

  private final Map<ODatabaseId, ODatabaseTopologyState> databases = new HashMap<>();
  private final Map<String, ODatabaseTopologyState> databasesByName = new HashMap<>();
  private final Map<ODatabaseId, ORawPair<OTransactionIdPromise, ODatabaseTopologyState>> promised =
      new HashMap<>();
  private final Map<String, ODatabaseTopologyState> promisedByName = new HashMap<>();
  private final Map<OSyncId, OSyncState> activerSyncs = new HashMap<>();
  private ODatabaseStateChangeListener listener;

  public ODatabasesTopologyState(ODatabaseStateChangeListener listener) {
    this.listener = listener;
  }

  public synchronized Optional<OAcceptResult> validateDeclare(
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
        var declared = new ODatabaseTopologyState(db, name, partecipants, minimumQuorum, listener);
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
      var declared = new ODatabaseTopologyState(db, name, partecipants, minimumQuorum, listener);
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
              return new ODatabaseTopologyState(db, name, Set.of(node), 0, listener);
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

  public synchronized Optional<OAcceptResult> validateSetState(
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

  public synchronized void cancelSetState(ODatabaseId dbId, ONodeId nodeId, long version) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      db.cancelSetState(nodeId, version);
    }
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

  public synchronized Optional<OSyncInfo> newSync(ODatabaseId dbId) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db == null) {
      return Optional.empty();
    }
    return db.newSync();
  }

  public synchronized Optional<OSyncState> canSync(
      ONodeId sender,
      ONodeId receiver,
      ODatabaseId dbId,
      OSyncId syncId,
      boolean canSync,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db == null) {
      return Optional.empty();
    }
    var state = db.canSync(sender, receiver, syncId, canSync, mode, sequenceStatus);
    if (state.isPresent()) {
      this.activerSyncs.put(state.get().getSyncId(), state.get());
    }
    return state;
  }

  public synchronized OSyncState startSend(
      ONodeId to,
      ONodeId from,
      ODatabaseId dbId,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db == null) {
      throw new NullPointerException("missing database definition");
    }
    OSyncState state = db.startSend(from, to, syncId, mode, sequenceStatus);
    this.activerSyncs.put(syncId, state);
    return state;
  }

  public synchronized OSyncState getSyncState(OSyncId syncId) {
    return this.activerSyncs.get(syncId);
  }

  public synchronized String getDatabaseName(ODatabaseId dbId) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      return db.getName();
    }
    return null;
  }

  public synchronized boolean acceptSync(
      ONodeId sender, ONodeId receiver, ODatabaseId dbId, OSyncId syncId) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      return db.acceptSync(sender, receiver, syncId);
    }
    return false;
  }

  public synchronized Optional<ODatabaseId> getDatabaseId(String databaseName) {
    ODatabaseTopologyState stat = this.databasesByName.get(databaseName);
    if (stat != null) {
      return Optional.of(stat.getId());
    } else {
      return Optional.empty();
    }
  }

  public synchronized boolean isMain(ODatabaseId dbId, ONodeId nodeId) {
    ODatabaseTopologyState stat = this.databases.get(dbId);
    if (stat != null) {
      return stat.isMain(nodeId);
    }
    return false;
  }

  public synchronized void receiverNetworkState(List<ODatabaseStateNetwork> network) {
    for (ODatabaseStateNetwork state : network) {
      ODatabaseTopologyState db = this.databases.get(state.getId());
      if (db != null) {
        db.receiveState(state);
      } else {
        db = new ODatabaseTopologyState(state, listener);
        this.databases.put(state.getId(), db);
        this.databasesByName.put(state.getName(), db);
      }
    }
  }

  public synchronized List<ODatabaseStateNetwork> getNetworkState() {
    List<ODatabaseStateNetwork> databases = new ArrayList<>();
    for (ODatabaseTopologyState state : this.databases.values()) {
      databases.add(state.getNetworkState());
    }
    return databases;
  }

  public synchronized Collection<ODatabaseId> getDatabases() {
    return new HashSet<>(this.databases.keySet());
  }

  public synchronized ODatabaseState getState(ODatabaseId dbId, ONodeId nodeID) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      return db.getState(nodeID);
    } else {
      return ODatabaseState.NotAvailable;
    }
  }

  public boolean isOnline(ODatabaseId dbId, ONodeId nodeID) {
    return ODatabaseState.Online.equals(getState(dbId, nodeID));
  }

  public synchronized boolean shouldSink(ODatabaseId dbId, ONodeId nodeID) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      return db.shouldSink(nodeID);
    } else {
      return false;
    }
  }

  public synchronized ONodeRole getRole(ODatabaseId dbId, ONodeId nodeId) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      return db.getRole(nodeId);
    } else {
      return null;
    }
  }

  public synchronized void mergeNetworkState(List<ODatabaseStateNetwork> network) {
    for (ODatabaseStateNetwork state : network) {
      ODatabaseTopologyState db = this.databases.get(state.getId());
      if (db != null) {
        db.mergeState(state);
      } else {
        db = new ODatabaseTopologyState(state, listener);
        this.databases.put(state.getId(), db);
        this.databasesByName.put(state.getName(), db);
      }
    }
  }

  public synchronized Optional<OAcceptResult> validateAddMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, long version) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      return db.promiseMember(nodes, version);
    } else {
      return Optional.of(new ODatabaseMissing());
    }
  }

  public synchronized void addDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, long version) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      db.addMember(nodes, version);
    }
  }

  public synchronized void cancelAddDatabaseMember(ODatabaseId dbId, List<OAddNodeInfo> nodes) {
    ODatabaseTopologyState db = this.databases.get(dbId);
    if (db != null) {
      db.cancelAddMemer(nodes);
    }
  }

  public synchronized void completeSync(OSyncId syncId) {
    OSyncState sync = this.activerSyncs.remove(syncId);
    if (sync != null) {
      ODatabaseTopologyState db = this.databases.get(sync.getDbId());
      if (db != null) {
        db.completeSync(syncId);
      }
    }
  }

  public synchronized ODatabasesTopologyStore getStore() {
    var dbs = this.databases.values().stream().map((x) -> x.getStore()).toList();
    return new ODatabasesTopologyStore(dbs);
  }

  public synchronized void load(ODatabasesTopologyStore store) {
    var dbs =
        store.getDatabases().stream().map(x -> new ODatabaseTopologyState(listener, x)).toList();
    for (var db : dbs) {
      this.databases.put(db.getId(), db);
      this.databasesByName.put(db.getName(), db);
    }
  }
}
