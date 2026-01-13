package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.ONetworkTopologyStore;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.OStateStore;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopologyStore;
import java.io.IOException;
import java.util.Optional;

public class OSystemStateStore implements OStateStore {
  private static final OLogger logger = OLogManager.instance().logger(OSystemStateStore.class);

  private OSystemDatabase systemDatabase;

  public OSystemStateStore(OSystemDatabase systemDatabase) {
    this.systemDatabase = systemDatabase;
    systemDatabase.executeWithDB(
        (session) -> {
          session.createClassIfNotExist("ODistributedNodeState");
          session.createClassIfNotExist("ODistributedSequenceState");
          session.createClassIfNotExist("ODistributedDatabasesState");
          return (Void) null;
        });
  }

  @Override
  public ONodeStateStore load() {

    return systemDatabase.executeWithDB(
        (session) -> {
          var sequence = loadSequence(session);
          var network = loadNetwork(session);
          var dbs = loadDatabases(session);
          return new ONodeStateStore(sequence, network, dbs);
        });
  }

  @Override
  public void save(ONodeStateStore store) {
    systemDatabase.executeWithDB(
        (session) -> {
          session.begin();
          if (store.sequence().isPresent()) {
            saveSequence(null, store.sequence().get(), session);
          }
          if (store.network().isPresent()) {
            saveNetwork(store.network().get(), session);
          }
          if (store.databases().isPresent()) {
            saveDatabases(store.databases().get(), session);
          }
          session.commit();
          return null;
        });
  }

  private Optional<ONetworkTopologyStore> loadNetwork(ODatabaseSession session) {
    try (OResultSet res = session.query("select * from ODistributedNodeState")) {
      if (res.hasNext()) {
        OResult d = res.next();
        return Optional.of(ONetworkTopologyStore.fromResult(d));
      }
    }
    return Optional.empty();
  }

  private void saveSequence(
      OTransactionId tx, OTransactionSequenceStatus seq, ODatabaseSession session) {
    try (OResultSet res = session.query("select * from ODistributedSequenceState")) {
      OElement el;
      if (res.hasNext()) {
        el = res.next().getElement().get();
      } else {
        el = session.newElement("ODistributedSequenceState");
      }
      try {
        el.setProperty("sequenceBytes", seq.store());
        session.save(el);
      } catch (IOException e) {
        logger.warn("error on serialization of sequence status", e);
      }
    }
  }

  private Optional<OTransactionSequenceStatus> loadSequence(ODatabaseSession session) {
    try (OResultSet res = session.query("select * from ODistributedSequenceState")) {
      if (res.hasNext()) {
        OResult d = res.next();
        try {
          return Optional.of(
              OTransactionSequenceStatus.read((byte[]) d.getProperty("sequenceBytes")));
        } catch (IOException e) {
          logger.warn("error on deserialization of sequence status", e);
        }
      }
    }
    return Optional.empty();
  }

  private void saveNetwork(ONetworkTopologyStore store, ODatabaseSession session) {
    try (OResultSet res = session.query("select * from ODistributedNodeState")) {
      OElement el;
      if (res.hasNext()) {
        el = res.next().getElement().get();
      } else {
        el = session.newElement("ODistributedNodeState");
      }
      store.toElement(el);
      session.save(el);
    }
  }

  private void saveDatabases(ODatabasesTopologyStore store, ODatabaseSession session) {
    try (OResultSet res = session.query("select * from ODistributedDatabasesState")) {
      OElement el;
      if (res.hasNext()) {
        el = res.next().getElement().get();
      } else {
        el = session.newElement("ODistributedDatabasesState");
      }
      store.toElement(el);
      session.save(el);
    }
  }

  private Optional<ODatabasesTopologyStore> loadDatabases(ODatabaseSession session) {
    try (OResultSet res = session.query("select * from ODistributedDatabasesState")) {
      if (res.hasNext()) {
        OResult d = res.next();
        return Optional.of(ODatabasesTopologyStore.fromResult(d));
      }
    }
    return Optional.empty();
  }
}
