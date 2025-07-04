package com.orientechnologies.orient.core.command.script;

import java.util.concurrent.ConcurrentHashMap;
import javax.script.ScriptEngine;

public class ODatabaseScriptPool {
  protected ConcurrentHashMap<String, ODatabaseScriptManager> dbManagers =
      new ConcurrentHashMap<String, ODatabaseScriptManager>();
  protected static final Object[] EMPTY_PARAMS = new Object[] {};
  private OScriptManager manager;

  public ODatabaseScriptPool(OScriptManager manager) {
    this.manager = manager;
  }

  /**
   * Acquires a database engine from the pool. Once finished using it, the instance MUST be returned
   * in the pool by calling the method #releaseDatabaseEngine(String, ScriptEngine).
   *
   * @param databaseName Database name
   * @param language Script language
   * @return ScriptEngine instance with the function library already parsed
   * @see #releaseDatabaseEngine(String, String, ScriptEngine)
   */
  public ScriptEngine acquireDatabaseEngine(final String databaseName, final String language) {
    ODatabaseScriptManager dbManager = dbManagers.get(databaseName);
    if (dbManager == null) {
      // CREATE A NEW DATABASE SCRIPT MANAGER
      dbManager = new ODatabaseScriptManager(manager, databaseName);
      final ODatabaseScriptManager prev = dbManagers.putIfAbsent(databaseName, dbManager);
      if (prev != null) {
        dbManager.close();
        // GET PREVIOUS ONE
        dbManager = prev;
      }
    }

    return dbManager.acquireEngine(language);
  }

  /**
   * Acquires a database engine from the pool. Once finished using it, the instance MUST be returned
   * in the pool by calling the method
   *
   * @param iLanguage Script language
   * @param iDatabaseName Database name
   * @param poolEntry Pool entry to free
   * @see #acquireDatabaseEngine(String, String)
   */
  public void releaseDatabaseEngine(
      final String iLanguage, final String iDatabaseName, final ScriptEngine poolEntry) {
    final ODatabaseScriptManager dbManager = dbManagers.get(iDatabaseName);
    // We check if there is still a valid pool because it could be removed by the function reload
    if (dbManager != null) {
      dbManager.releaseEngine(iLanguage, poolEntry);
    }
  }

  public void close(String database) {
    final ODatabaseScriptManager dbPool = dbManagers.remove(database);
    if (dbPool != null) dbPool.close();
  }

  public void closeAll() {
    dbManagers.entrySet().forEach(e -> e.getValue().close());
  }
}
