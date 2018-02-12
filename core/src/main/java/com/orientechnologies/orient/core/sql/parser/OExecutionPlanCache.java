package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already prepared SQL execution plans. It stores itself in the storage as a resource. It also acts
 * an an entry point for the SQL executor.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OExecutionPlanCache {

  Map<String, OInternalExecutionPlan> map;
  int                                 mapSize;

  /**
   * @param size the size of the cache
   */
  public OExecutionPlanCache(int size) {
    this.mapSize = size;
    map = new LinkedHashMap<String, OInternalExecutionPlan>(size) {
      protected boolean removeEldestEntry(final Map.Entry<String, OInternalExecutionPlan> eldest) {
        return super.size() > mapSize;
      }
    };
  }

  /**
   * @param statement an SQL statement
   *
   * @return true if the corresponding executor is present in the cache
   */
  public boolean contains(String statement) {
    synchronized (map) {
      return map.containsKey(statement);
    }
  }

  /**
   * returns an already prepared SQL execution plan, taking it from the cache if it exists or creating a new one if it doesn't
   *
   * @param statement the SQL statement
   * @param db        the current DB instance
   *
   * @return a statement executor from the cache
   */
  public static OExecutionPlan get(String statement, Map params, ODatabaseDocumentInternal db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }

    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
    return resource.getOrCreate(statement, params, false, db);
  }

  public static OExecutionPlan get(String statement, Object[] params, ODatabaseDocumentInternal db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }

    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
    return resource.getOrCreate(statement, params, false, db);
  }

  /**
   * @param statement an SQL statement
   *
   * @return the corresponding executor, taking it from the internal cache, if it exists
   */
  public OExecutionPlan getOrCreate(String statement, Map params, boolean profile, ODatabaseDocumentInternal db) {
    OInternalExecutionPlan result;
    synchronized (map) {
      //LRU
      result = map.remove(statement);
      if (result != null) {
        map.put(statement, result);
      }
    }
    if (result == null) {
      OStatement stm = db.getSharedContext().getStatementCache().get(statement);
      OBasicCommandContext ctx = new OBasicCommandContext();
      ctx.setDatabase(db);
      ctx.setInputParameters(params);
      result = stm.createExecutionPlan(ctx, profile);
      if (stm.executinPlanCanBeCached()) {
        synchronized (map) {
          map.put(statement, result);
        }
        result = result.copy(ctx);
      }
    }
    return result;
  }

  public OExecutionPlan getOrCreate(String statement, Object[] args, boolean profile, ODatabaseDocumentInternal db) {
    OInternalExecutionPlan result;
    synchronized (map) {
      //LRU
      result = map.remove(statement);
      if (result != null) {
        map.put(statement, result);
      }
    }

    OBasicCommandContext ctx = new OBasicCommandContext();
    ctx.setDatabase(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);

    boolean cached = false;
    if (result == null) {
      OStatement stm = db.getSharedContext().getStatementCache().get(statement);

      result = stm.createExecutionPlan(ctx, profile);
      if (stm.executinPlanCanBeCached()) {
        synchronized (map) {
          map.put(statement, result);
        }
        cached = true;
      }
    }else{
      cached = true;
    }
    if (cached) {
      result = result.copy(ctx);
      result.reset(ctx);
    }
    return result;
  }

  protected static void throwParsingException(ParseException e, String statement) {
    throw new OCommandSQLParsingException(e, statement);
  }

  protected static void throwParsingException(TokenMgrError e, String statement) {
    throw new OCommandSQLParsingException(e, statement);
  }

}
