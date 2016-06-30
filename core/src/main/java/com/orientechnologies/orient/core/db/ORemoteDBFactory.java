package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 08/04/16.
 */
public class ORemoteDBFactory implements OrientDBFactory {
  private Map<String, OStorage> storages = new HashMap<>();
  private OEngine remote;

  public ORemoteDBFactory(String[] hosts, OrientDBSettings configuration) {
    super();
    remote = Orient.instance().getEngine("remote");
  }

  private String buildUrl(String name) {
    return null;
  }

  @Override
  public synchronized ODatabaseDocument open(String name, String user, String password) {
    OStorage storage = storages.get(name);
    if (storage == null) {
      storage = remote.createStorage(buildUrl(name), new HashMap<>());
    }
    ODatabaseDocumentRemote db = new ODatabaseDocumentRemote(storage);
    db.internalOpen(user, password);
    return db;
  }

  @Override
  public synchronized void create(String name, String user, String password, DatabaseType databaseType) {

  }

  @Override
  public synchronized boolean exist(String name, String user, String password) {
    return false;
  }

  @Override
  public synchronized void drop(String name, String user, String password) {

  }

  @Override
  public Map<String, String> listDatabases(String user, String password) {
    return null;
  }

  @Override
  public OPool<ODatabaseDocument> openPool(String name, String user, String password, Map<String, Object> poolSettings) {
    return null;
  }

  @Override
  public void close() {

  }
}
