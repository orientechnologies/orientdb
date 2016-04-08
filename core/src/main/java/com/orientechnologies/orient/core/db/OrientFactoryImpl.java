package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;

import java.util.Map;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientFactoryImpl extends OrientFactory {

  public enum FactoryType {
    EMBEDDED, REMOTE, DISTRIBUTED
  }

  private FactoryType type;
  private String      baseUrl;

  public OrientFactoryImpl(FactoryType type, String baseUrl) {
    this.type = type;
    this.baseUrl = baseUrl;
  }

  @Override
  public ODatabaseDocument open(String name, String user, String password) {
    ODatabaseDocument database;
    switch (type) {
    case  EMBEDDED:
      database = new ODatabaseDocumentTx("plocal:"+baseUrl+"/"+name);
      break;
    case REMOTE:
      database = new ODatabaseDocumentTx("remote:"+baseUrl+"/"+name);
      break;
    case DISTRIBUTED:
      // not sure how do this.
    default:
      throw new ODatabaseException("not supported ");
    }
    database.open(user,password);
    return database;
  }

  @Override
  public void create(String name, String user, String password, DatabaseType databaseType) {
    switch (type) {
    case  EMBEDDED:
      ODatabaseDocumentTx database = new ODatabaseDocumentTx("plocal:" + baseUrl + "/" + name);
      database.create();
      break;
    case REMOTE:
//      IMplementation with oServer Admin.
      break;
    case DISTRIBUTED:
      // not sure how do this.
    default:
      throw new ODatabaseException("not supported ");
    }
  }

  @Override
  public boolean exist(String name, String user, String password) {
    switch (type) {
    case  EMBEDDED:
      ODatabaseDocumentTx database = new ODatabaseDocumentTx("plocal:" + baseUrl + "/" + name);
      return database.exists();
    case REMOTE:
      //      IMplementation with oServer Admin.
      return false;
    case DISTRIBUTED:
      // not sure how do this.
    default:
      throw new ODatabaseException("not supported ");
    }

  }

  @Override
  public void drop(String name, String user, String password) {
    switch (type) {
    case  EMBEDDED:
      ODatabaseDocumentTx database = new ODatabaseDocumentTx("plocal:" + baseUrl + "/" + name);
      database.drop();
    case REMOTE:
      //      IMplementation with oServer Admin.
      break;
    case DISTRIBUTED:
      // not sure how do this.
    default:
      throw new ODatabaseException("not supported ");
    }

  }

  @Override
  public Map<String, String> listDatabases(String user, String password) {
    //TODO
    return null;
  }

  @Override
  public void close() {

  }
}
