package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already parsed SQL statement executors. It stores itself in the
 * storage as a resource. It also acts an an entry point for the SQL parser.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OStatementCache {

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
      return parse(statement);
    }

    OStatementCache resource = db.getSharedContext().getStatementCache();
    return resource.get(statement);
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
  public static OServerStatement getServerStatement(String statement, OrientDBInternal db) {
    // TODO create a global cache!
    return parseServerStatement(statement);
  }
  /**
   * @param statement an SQL statement
   * @return the corresponding executor, taking it from the internal cache, if it exists
   */
  public OStatement get(String statement) {
    if (OGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger() == 0) {
      return parse(statement);
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
      result = parse(statement);
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
   * @return the corresponding executor
   * @throws OCommandSQLParsingException if the input parameter is not a valid SQL statement
   */
  protected static OStatement parse(String statement) throws OCommandSQLParsingException {
    try {
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      InputStream is;

      if (db == null) {
        is = new ByteArrayInputStream(statement.getBytes());
      } else {
        try {
          is =
              new ByteArrayInputStream(
                  statement.getBytes(db.getStorageInfo().getConfiguration().getCharset()));
        } catch (UnsupportedEncodingException e2) {
          OLogManager.instance()
              .warn(
                  null,
                  "Unsupported charset for database "
                      + db
                      + " "
                      + db.getStorageInfo().getConfiguration().getCharset());
          is = new ByteArrayInputStream(statement.getBytes());
        }
      }

      OrientSql osql = null;
      if (db == null) {
        osql = new OrientSql(is);
      } else {
        try {
          osql = new OrientSql(is, db.getStorageInfo().getConfiguration().getCharset());
        } catch (UnsupportedEncodingException e2) {
          OLogManager.instance()
              .warn(
                  null,
                  "Unsupported charset for database "
                      + db
                      + " "
                      + db.getStorageInfo().getConfiguration().getCharset());
          osql = new OrientSql(is);
        }
      }
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
  protected static OServerStatement parseServerStatement(String statement)
      throws OCommandSQLParsingException {
    try {
      InputStream is = new ByteArrayInputStream(statement.getBytes());
      OrientSql osql = new OrientSql(is);
      OServerStatement result = osql.parseServerStatement();
      //      result.originalStatement = statement;

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
