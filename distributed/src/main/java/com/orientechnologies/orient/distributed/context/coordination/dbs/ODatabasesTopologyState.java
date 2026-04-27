package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.OVersionPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ODatabaseStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.ODatabaseMissing;
import com.orientechnologies.orient.distributed.context.coordination.result.ODatabaseNameUsed;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ODatabasesTopologyState extends OWatcher implements ODatabasesTopology {

  private final ONodeId current;
  private final Map<ODatabaseId, ODatabaseTopologyState> databases = new HashMap<>();
  private final Map<String, ODatabaseTopologyState> databasesByName = new HashMap<>();
  private final Map<ODatabaseId, ORawPair<OVersionPromise, ODatabaseTopologyState>> promised =
      new HashMap<>();
  private final Map<String, ODatabaseTopologyState> promisedByName = new HashMap<>();
  private ODatabaseStateChangeListener listener;

  public ODatabasesTopologyState(ODatabaseStateChangeListener listener, ONodeId current) {
    this.listener = listener;
    this.current = current;
  }

  public synchronized Optional<OAcceptResult> validateDeclare(
      OTransactionIdPromise promise,
      ODatabaseId db,
      String name,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum) {

    if (databases.containsKey(db)) {
      var def = getDb(db);
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
          return prom.getFirst().promise(promise, new OVersion(1));
        }
        if (promisedByName.containsKey(name)) {
          var prom = promised.get(promisedByName.get(name).getId());
          return prom.getFirst().promise(promise, new OVersion(1));
        }
        var declared =
            new ODatabaseTopologyState(db, name, partecipants, minimumQuorum, listener, current);
        var version = new OVersionPromise(new OVersion(0), current);
        version.promise(promise, new OVersion(1));
        this.promised.put(
            db, new ORawPair<OVersionPromise, ODatabaseTopologyState>(version, declared));
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
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum) {
    if (promised.containsKey(db)) {
      var prom = promised.remove(db);
      var inst = prom.getSecond();
      promisedByName.remove(name);
      assert inst.getName().equals(name);
      this.databases.put(db, inst);
      this.databasesByName.put(inst.getName(), inst);
    } else {
      var declared =
          new ODatabaseTopologyState(db, name, partecipants, minimumQuorum, listener, current);
      this.databases.put(db, declared);
      this.databasesByName.put(declared.getName(), declared);
    }
    this.notifyAll();
  }

  public synchronized void setState(
      ODatabaseId db,
      ONodeId node,
      ODatabaseState state,
      OVersion version,
      OTransactionIdPromise promise) {
    var nodes = getDb(db);
    if (nodes != null) {
      nodes.setState(node, state, version, promise);
    }
  }

  public synchronized Set<ODatabaseId> listDatabaseIds() {
    return this.databases.keySet();
  }

  public synchronized Optional<OAcceptResult> validateSetState(
      ODatabaseId dbId,
      ONodeId nodeId,
      ODatabaseState state,
      OVersion version,
      OTransactionIdPromise promise) {
    ODatabaseTopologyState dbTopology = getDb(dbId);
    if (dbTopology != null) {
      return dbTopology.promiseState(state, nodeId, version, promise);
    } else {
      return Optional.of(new ODatabaseMissing(dbId));
    }
  }

  public synchronized OVersion getDatabaseVersion(ODatabaseId dbId) {
    ODatabaseTopologyState dbTopology = getDb(dbId);
    if (dbTopology != null) {
      return dbTopology.getVersion();
    } else {
      return new OVersion(0);
    }
  }

  public synchronized void cancelSetState(
      ODatabaseId dbId, ONodeId nodeId, OVersion version, OTransactionIdPromise promise) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      db.cancelSetState(nodeId, version, promise);
    }
  }

  public synchronized boolean waitOnline(ODatabaseId dbId, ONodeId nodeId) {
    return false;
  }

  public boolean waitOnlineQuorum(ODatabaseId dbId, Optional<Long> timeout)
      throws InterruptedException {
    ODatabaseTopologyState db;
    synchronized (this) {
      db = getDb(dbId);
    }
    if (db != null) {
      return db.waitOnlineQuorum(timeout);
    }
    return false;
  }

  private synchronized ODatabaseTopologyState getDb(ODatabaseId dbId) {
    return this.databases.get(dbId);
  }

  public boolean waitSelfOnline(String dbName, Optional<Long> timeout) throws InterruptedException {
    ODatabaseTopologyState db;
    synchronized (this) {
      db = this.databasesByName.get(dbName);
    }
    if (db != null) {
      return db.waitSelfOnline(timeout);
    } else {
      if (waitFor(timeout, () -> this.databasesByName.containsKey(dbName))) {
        synchronized (this) {
          db = this.databasesByName.get(dbName);
        }
        return db.waitSelfOnline(timeout);
      }
    }
    return false;
  }

  public boolean waitSelfOnline(ODatabaseId dbId, Optional<Long> timeout)
      throws InterruptedException {
    ODatabaseTopologyState db;
    synchronized (this) {
      db = getDb(dbId);
    }
    if (db != null) {
      return db.waitSelfOnline(timeout);
    } else {
      if (waitFor(timeout, () -> this.databases.containsKey(dbId))) {
        return getDb(dbId).waitSelfOnline(timeout);
      }
    }
    return false;
  }

  public boolean waitOnlineOne(ODatabaseId dbId) {
    ODatabaseTopologyState db;
    synchronized (this) {
      db = getDb(dbId);
    }
    if (db != null) {
      return db.waitOnlineOne();
    }
    return false;
  }

  public synchronized boolean executeOnOneOnline(ODatabaseId dbId, ONotificationAction execute) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      db.executeOnOneOnline(execute);
      return true;
    }
    return false;
  }

  public synchronized Set<ONodeId> getOnlineNodes(ODatabaseId dbId) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      return db.getOnlineNodes();
    }
    return Collections.emptySet();
  }

  public synchronized Optional<OSyncInfo> newSync(ODatabaseId dbId) {
    ODatabaseTopologyState db = getDb(dbId);
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
    ODatabaseTopologyState db = getDb(dbId);
    if (db == null) {
      return Optional.empty();
    }
    var state = db.canSync(sender, receiver, syncId, canSync, mode, sequenceStatus);
    return state;
  }

  public synchronized OSyncState startSend(
      ONodeId to,
      ONodeId from,
      ODatabaseId dbId,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db == null) {
      throw new NullPointerException("missing database definition");
    }
    return db.startSend(from, to, syncId, mode, sequenceStatus);
  }

  public synchronized OSyncState getSyncState(OSyncId syncId) {
    ODatabaseTopologyState db = getDb(syncId.getDbId());
    if (db != null) {
      return db.getSyncState(syncId);
    } else {
      return null;
    }
  }

  public synchronized String getDatabaseName(ODatabaseId dbId) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      return db.getName();
    }
    return null;
  }

  public synchronized boolean acceptSync(
      ONodeId sender, ONodeId receiver, ODatabaseId dbId, OSyncId syncId) {
    ODatabaseTopologyState db = getDb(dbId);
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
    ODatabaseTopologyState stat = getDb(dbId);
    if (stat != null) {
      return stat.isMain(nodeId);
    }
    return false;
  }

  public synchronized void receiverNetworkState(List<ODatabaseStateNetwork> network) {
    for (ODatabaseStateNetwork state : network) {
      ODatabaseTopologyState db = getDb(state.id());
      if (db != null) {
        db.receiveState(state, true);
      } else {
        db = new ODatabaseTopologyState(state, listener, current);
        this.databases.put(state.id(), db);
        this.databasesByName.put(state.name(), db);
        db.notifyAllNodesStates();
      }
    }
    this.notifyAll();
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
    ODatabaseTopologyState db = getDb(dbId);
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
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      return db.shouldSink(nodeID);
    } else {
      return false;
    }
  }

  public synchronized ONodeRole getRole(ODatabaseId dbId, ONodeId nodeId) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      return db.getRole(nodeId);
    } else {
      return null;
    }
  }

  public synchronized void mergeNetworkState(
      List<ODatabaseStateNetwork> network, OTransactionIdPromise promise) {
    for (ODatabaseStateNetwork state : network) {
      ODatabaseTopologyState db = getDb(state.id());
      if (db != null) {
        db.mergeState(state, promise);
      } else {
        db = new ODatabaseTopologyState(state, listener, current);
        this.databases.put(state.id(), db);
        this.databasesByName.put(state.name(), db);
        db.notifyAllNodesStates();
      }
    }
    this.notifyAll();
  }

  public synchronized Optional<OAcceptResult> validateAddMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OVersion version, OTransactionIdPromise promise) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      return db.promiseMember(nodes, version, promise);
    } else {
      return Optional.of(new ODatabaseMissing(dbId));
    }
  }

  public synchronized void addDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OVersion version, OTransactionIdPromise promise) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      db.addMember(nodes, version, promise);
    }
  }

  public synchronized void cancelAddDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OTransactionIdPromise promise) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      db.cancelAddMemer(nodes, promise);
    }
  }

  public synchronized Optional<OAcceptResult> validateRemoveMembers(
      ODatabaseId dbId, List<ONodeId> nodes, OVersion version, OTransactionIdPromise promise) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      return db.promiseRemoveMember(nodes, version, promise);
    } else {
      return Optional.of(new ODatabaseMissing(dbId));
    }
  }

  public synchronized void removeDatabaseMembers(
      ODatabaseId dbId, List<ONodeId> nodes, OVersion version, OTransactionIdPromise promise) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      db.removeMember(nodes, version, promise);
    }
  }

  public synchronized void cancelRemoveDatabaseMembers(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OTransactionIdPromise promise) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      db.cancelRemoveMemer(nodes, promise);
    }
  }

  public synchronized void completeSync(OSyncId syncId) {
    ODatabaseTopologyState db = getDb(syncId.getDbId());
    if (db != null) {
      db.completeSync(syncId);
    }
  }

  public synchronized ODatabasesTopologyStore getStore() {
    var dbs = this.databases.values().stream().map((x) -> x.getStore()).toList();
    return new ODatabasesTopologyStore(dbs);
  }

  public synchronized void load(ODatabasesTopologyStore store) {
    var dbs =
        store.getDatabases().stream()
            .map(x -> new ODatabaseTopologyState(listener, x, current))
            .toList();
    for (var db : dbs) {
      this.databases.put(db.getId(), db);
      this.databasesByName.put(db.getName(), db);
    }
  }

  public int getQuorum(ODatabaseId databaseId) {
    ODatabaseTopologyState db = getDb(databaseId);
    if (db != null) {
      return db.getQuorum();
    }
    return -1;
  }

  public synchronized Set<OSyncState> getActiveSyncs(ODatabaseId dbId) {
    ODatabaseTopologyState db = getDb(dbId);
    if (db != null) {
      return db.getSyncs();
    } else {
      return Collections.emptySet();
    }
  }

  public synchronized Optional<OAcceptResult> validateDropDatabase(
      OTransactionIdPromise promise, ODatabaseId dbId, OVersion version) {
    var db = getDb(dbId);
    if (db != null) {
      return db.validateDrop(promise, version);
    } else {
      // Database is missing here ... fair we can drop it
      return Optional.empty();
    }
  }

  public synchronized void dropDatabase(
      OTransactionIdPromise promise, ODatabaseId dbId, OVersion version) {
    var db = this.databases.remove(dbId);
    if (db != null) {
      this.databasesByName.remove(db.getName());
      /// Just .... because
      db.drop(promise, version);
    }
    this.notifyAll();
  }

  public void cancelDropDatabase(
      ODatabaseId dbId, OVersion version, OTransactionIdPromise promise) {
    var db = getDb(dbId);
    if (db != null) {
      db.cancelDrop(promise, version);
    }
  }

  public synchronized void nodeDisconnected(ONodeId node) {
    for (var db : this.databases.values()) {
      db.nodeDisconnected(node);
    }
  }

  public synchronized void cancelMerge(OTransactionIdPromise promise) {
    for (var db : this.databases.values()) {
      db.cancelMerge(promise);
    }
  }

  public synchronized Optional<OAcceptResult> validateMergeToNetwork(
      List<ODatabaseStateNetwork> original, OTransactionIdPromise promise) {
    var promised = new ArrayList<ODatabaseTopologyState>();
    var promisedId = new HashSet<ODatabaseId>();
    for (var originalDb : original) {
      Optional<OAcceptResult> res;
      var db = this.databases.get(originalDb.id());
      if (db != null) {
        res = db.validateMerge(promise, originalDb);
        if (res.isEmpty()) {
          promised.add(db);
          promisedId.add(originalDb.id());
        }
      } else {
        res = Optional.of(new ODatabaseMissing(originalDb.id()));
      }
      if (res.isPresent()) {
        for (var dbp : promised) {
          dbp.cancelMerge(promise);
        }
        return res;
      }
    }

    if (promisedId.size() != this.databases.size()) {
      for (var dbp : promised) {
        dbp.cancelMerge(promise);
      }
      var dbs = new HashSet<>(this.databases.keySet());
      dbs.removeAll(promisedId);
      return Optional.of(new ODatabaseMissing(dbs.iterator().next()));
    }

    return Optional.empty();
  }

  public synchronized OVersion nextDatabaseVersion(ODatabaseId id) {
    ODatabaseTopologyState dbTopology = getDb(id);
    if (dbTopology != null) {
      return dbTopology.nextVersion();
    } else {
      return new OVersion(0);
    }
  }

  public synchronized Optional<OAcceptResult> validateSetDatabaseNodeRole(
      ODatabaseId db,
      ONodeId node,
      ONodeRole role,
      OVersion version,
      OTransactionIdPromise promise) {
    ODatabaseTopologyState dbTopology = getDb(db);
    if (dbTopology != null) {
      return dbTopology.validateRole(node, version, promise);
    } else {
      return Optional.of(new ODatabaseMissing(db));
    }
  }

  public synchronized void setDatabaseNodeRole(
      ODatabaseId db,
      ONodeId node,
      ONodeRole role,
      OVersion version,
      OTransactionIdPromise promise) {
    ODatabaseTopologyState dbTopology = getDb(db);
    if (dbTopology != null) {
      dbTopology.setRole(node, role, version, promise);
    }
  }

  public synchronized void cancelSetDatabaseNodeRole(
      ODatabaseId db,
      ONodeId node,
      ONodeRole role,
      OVersion version,
      OTransactionIdPromise promise) {
    ODatabaseTopologyState dbTopology = getDb(db);
    if (dbTopology != null) {
      dbTopology.cancelRole(node, version, promise);
    }
  }

  public void dbRemovedFromDiskWhenOffline(ODatabaseId db) {
    var rd = this.databases.remove(db);
    this.databasesByName.remove(rd.getName());
  }

  @Override
  public Set<ONodeId> getMembers(ODatabaseId databaseId) {
    ODatabaseTopologyState db = getDb(databaseId);
    if (db != null) {
      return db.getMembers();
    }
    return Set.of();
  }

  public Optional<OAcceptResult> validateSetDatabaseQuorum(
      ODatabaseId db, int quorum, OVersion version, OTransactionIdPromise promise) {
    ODatabaseTopologyState dbTopology = getDb(db);
    if (dbTopology != null) {
      return dbTopology.validateSetQurum(quorum, version, promise);
    } else {
      return Optional.of(new ODatabaseMissing(db));
    }
  }

  public void setDatabaseQuorum(
      ODatabaseId db, int quorum, OVersion version, OTransactionIdPromise promise) {
    ODatabaseTopologyState dbTopology = getDb(db);
    if (dbTopology != null) {
      dbTopology.setQuorum(quorum, version, promise);
    }
  }

  public void cancelSetDatabaseQuorum(
      ODatabaseId db, int quorum, OVersion version, OTransactionIdPromise promise) {
    ODatabaseTopologyState dbTopology = getDb(db);
    if (dbTopology != null) {
      dbTopology.cancelQuorum(version, promise);
    }
  }

  public synchronized Optional<OAcceptResult> validateMergeNode(
      List<ODatabaseStateNetwork> databases, OTransactionIdPromise promise) {
    var promised = new HashSet<ODatabaseTopologyState>();
    var promisedId = new HashSet<ODatabaseId>();

    for (var stateDb : databases) {
      var db = this.databases.get(stateDb.id());
      if (db != null) {
        var res = db.validateMergeNode(promise, stateDb);
        if (res.isEmpty()) {
          promised.add(db);
          promisedId.add(db.getId());
        } else {
          for (var dbp : promised) {
            dbp.cancelMerge(promise);
          }
          return res;
        }
      } else {
        promisedId.add(stateDb.id());
      }
    }
    if (promised.size() != this.databases.size()) {
      for (var dbp : promised) {
        dbp.cancelMerge(promise);
      }
      var dbs = new HashSet<>(this.databases.keySet());
      dbs.removeAll(promisedId);
      return Optional.of(new ODatabaseMissing(dbs.iterator().next()));
    }

    return Optional.empty();
  }
}
