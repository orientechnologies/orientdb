package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already parsed SQL statement executors. It stores itself in the
 * storage as a resource. It also acts an an entry point for the SQL parser.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OStatementCache {
  private static final OLogger logger = OLogManager.instance().logger(OStatementCache.class);

  private Map<String, OStatement> map;
  private int mapSize;

  /** @param size the size of the cache */
  public OStatementCache(int size) {
    this.mapSize = size;
    map =
        new LinkedHashMap<String, OStatement>(size) {
          protected boolean removeEldestEntry(final Map.Entry<String, OStatement> eldest) {
            return super.size() > mapSize;
          }
        };
  }

  /**
   * @param statement an SQL statement
   * @return true if the corresponding executor is present in the cache
   */
  public boolean contains(String statement) {
    if (OGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger() == 0) {
      return false;
    }

    synchronized (map) {
      return map.containsKey(statement);
    }
  }

  /**
   * returns an already parsed SQL executor, taking it from the cache if it exists or creating a new
   * one (parsing and then putting it into the cache) if it doesn't
   *
   * @param statement the SQL statement
   * @param db the current DB instance. If null, cache is ignored and a new executor is created
   *     through statement parsing
   * @return a statement executor from the cache
   */
  public static OStatement get(String statement, ODatabaseDocumentInternal db) {
    if (db == null) {
      return parse(statement, db);
    }

    OStatementCache resource = ((OSharedContextEmbedded) db.getSharedContext()).getStatementCache();
    return resource.getStatement(statement, db);
  }

  /**
   * returns an already parsed server-level SQL executor, taking it from the cache if it exists or
   * creating a new one (parsing and then putting it into the cache) if it doesn't
   *
   * @param statement the SQL statement
   * @param db the current OrientDB instance. If null, cache is ignored and a new executor is
   *     created through statement parsing
   * @return a statement executor from the cache
   */
  public static OAdminStatement getAdminStatement(String statement, OrientDBInternal db) {
    // TODO create a global cache!
    return parseAdminStatement(statement);
  }

  /**
   * @param statement an SQL statement
   * @return the corresponding executor, taking it from the internal cache, if it exists
   */
  public OStatement getStatement(String statement, ODatabaseDocumentInternal db) {
    if (OGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger() == 0) {
      return parse(statement, db);
    }

    OStatement result;
    synchronized (map) {
      // LRU
      result = map.remove(statement);
      if (result != null) {
        map.put(statement, result);
      }
    }
    if (result == null) {
      result = parse(statement, db);
      synchronized (map) {
        map.put(statement, result);
      }
    }
    return result;
  }

  /**
   * parses an SQL statement and returns the corresponding executor
   *
   * @param statement the SQL statement
   * @param db TODO
   * @return the corresponding executor
   * @throws OCommandSQLParsingException if the input parameter is not a valid SQL statement
   */
  protected static OStatement parse(String statement, ODatabaseDocumentInternal db)
      throws OCommandSQLParsingException {
    try {

      OrientSql osql = new OrientSql(statement);
      OStatement result = osql.parse();
      result.originalStatement = statement;

      return result;
    } catch (ParseException e) {
      throwParsingException(e, statement);
    } catch (TokenMgrError e2) {
      throwParsingException(e2, statement);
    }
    return null;
  }

  /**
   * parses an SQL statement and returns the corresponding executor
   *
   * @param statement the SQL statement
   * @return the corresponding executor
   * @throws OCommandSQLParsingException if the input parameter is not a valid SQL statement
   */
  protected static OAdminStatement parseAdminStatement(String statement)
      throws OCommandSQLParsingException {
    try {
      OrientSql osql = new OrientSql(statement);
      OAdminStatement result = osql.parseAdminStatement();
      return result;
    } catch (ParseException e) {
      throwParsingException(e, statement);
    } catch (TokenMgrError e2) {
      throwParsingException(e2, statement);
    }
    return null;
  }

  protected static void throwParsingException(ParseException e, String statement) {
    throw new OCommandSQLParsingException(e, statement);
  }

  protected static void throwParsingException(TokenMgrError e, String statement) {
    throw new OCommandSQLParsingException(e, statement);
  }

  public void clear() {
    if (OGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger() == 0) {
      return;
    }

    synchronized (map) {
      map.clear();
    }
  }
}
