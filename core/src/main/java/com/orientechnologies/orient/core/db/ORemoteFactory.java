package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.engine.OEngine;

import java.util.Map;

/**
 * Created by tglman on 08/04/16.
 */
public class ORemoteFactory extends OrientFactory {
  //need remote package
  private OEngine engine = null;

  public ORemoteFactory(String[] hosts) {
    super();
  }

  @Override
  public ODatabaseDocument open(String name, String user, String password) {
    return null;
  }

  @Override
  public void create(String name, String user, String password, DatabaseType databaseType) {

  }

  @Override
  public boolean exist(String name, String user, String password) {
    return false;
  }

  @Override
  public void drop(String name, String user, String password) {

  }

  @Override
  public Map<String, String> listDatabases(String user, String password) {
    return null;
  }

  @Override
  public Pool<ODatabaseDocument> open(String name, String user, String password, Map<String, Object> poolSettings) {
    return null;
  }

  @Override
  public void close() {

  }
}
