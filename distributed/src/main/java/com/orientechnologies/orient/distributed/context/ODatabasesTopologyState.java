package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.ODatabaseNameUsed;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ODatabasesTopologyState {

  private final Map<ODatabaseId, ODatabaseTopologyState> databases = new HashMap<>();
  private final Map<String, ODatabaseTopologyState> databasesByName = new HashMap<>();
  private final Map<ODatabaseId, ORawPair<OTransactionIdPromise, ODatabaseTopologyState>> promised =
      new HashMap<>();
  private final Map<String, ODatabaseTopologyState> promisedByName = new HashMap<>();

  public ODatabasesTopologyState() {}

  public synchronized Optional<OAcceptResult> promiseDeclare(
      OTransactionIdPromise promise, ODatabaseId db, String name) {

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
        var declared = new ODatabaseTopologyState(db, name);
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
      OTransactionIdPromise promise, ODatabaseId db, String name) {
    if (promised.containsKey(db)) {
      var prom = promised.remove(db);
      var inst = prom.getSecond();
      promisedByName.remove(name);
      assert inst.getName().equals(name);
      this.databases.put(db, inst);
      this.databasesByName.put(inst.getName(), inst);
    } else {
      var declared = new ODatabaseTopologyState(db, name);
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
              return new ODatabaseTopologyState(db, name);
            });

    // First declare, version 0
    nodes.defineNode(node, role, state, 0);
  }

  public synchronized void setState(
      ODatabaseId db, ONodeId node, ODatabaseState state, int version) {
    var nodes = databases.get(db);
    if (nodes != null) {
      nodes.setState(node, state, version);
    }
  }

  public synchronized Set<ODatabaseId> listDatabaseIds() {
    return this.databases.keySet();
  }
}
