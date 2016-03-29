package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

import java.util.Map;

/**
 * Created by tglman on 27/03/16.
 */
public abstract class OrientFactory {

  public static OrientFactory join() {
    return null;
  }

  public static OrientFactory remote(String url) {
    return null;
  }

  public static OrientFactory embedded(String url){
    return null;
  }

  public abstract ODatabaseDocument open(String name, String user, String password);

  public abstract void create(String name, String user, String password);

  public abstract boolean exist(String name, String user, String password);

  public abstract boolean drop(String name, String user, String password);

  public abstract Map<String, String> listDatabases(String user, String password);


}
