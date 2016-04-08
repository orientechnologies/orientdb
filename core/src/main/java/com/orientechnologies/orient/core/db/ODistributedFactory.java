package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

import java.util.Map;

/**
 * Created by tglman on 08/04/16.
 */
public class ODistributedFactory extends OrientFactory {
  public ODistributedFactory(String[] nodes) {
    super();
  }

  @Override
  public ODatabaseDocument open(String name, String user, String password) {
    return null;
  }

  @Override
  public void create(String name, String user, String password, DatabaseType type) {

  }

  public /*OServer*/ Object spawnServer(/*OServerConfiguration*/Object serverConfiguration){
    return null;
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
  public void close() {

  }
}
