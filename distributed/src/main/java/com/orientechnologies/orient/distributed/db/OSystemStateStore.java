package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.OStateStore;
import java.util.Optional;

public class OSystemStateStore implements OStateStore {

  private OSystemDatabase systemDatabase;

  public OSystemStateStore(OSystemDatabase systemDatabase) {
    this.systemDatabase = systemDatabase;
    systemDatabase.executeWithDB(
        (session) -> {
          session.createClassIfNotExist("ODistributedNodeState");
          session.createClassIfNotExist("ODistributedSequenceState");
          return (Void) null;
        });
  }

  @Override
  public Optional<ONodeStateStore> loadState() {
    Optional<ONodeStateStore> ret =
        systemDatabase.executeWithDB(
            (session) -> {
              try (OResultSet res = session.query("select * from ODistributedNodeState")) {
                if (res.hasNext()) {
                  OResult d = res.next();
                  return Optional.of(ONodeStateStore.fromResult(d));
                }
              }
              return Optional.empty();
            });
    return ret;
  }

  @Override
  public void saveSequence(byte[] seq) {
    systemDatabase.executeWithDB(
        (session) -> {
          try (OResultSet res = session.query("select * from ODistributedSequenceState")) {
            OElement el;
            if (res.hasNext()) {
              el = res.next().getElement().get();
            } else {
              el = session.newElement("ODistributedSequenceState");
            }
            el.setProperty("sequenceBytes", seq);
            session.save(el);
          }
          return (Void) null;
        });
  }

  @Override
  public Optional<byte[]> loadSequence() {
    Optional<byte[]> ret =
        systemDatabase.executeWithDB(
            (session) -> {
              try (OResultSet res = session.query("select * from ODistributedSequenceState")) {
                if (res.hasNext()) {
                  OResult d = res.next();
                  return Optional.of(d.getProperty("sequenceBytes"));
                }
              }
              return Optional.empty();
            });
    return ret;
  }

  @Override
  public void saveState(ONodeStateStore store) {
    systemDatabase.executeWithDB(
        (session) -> {
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
          return (Void) null;
        });
  }
}
