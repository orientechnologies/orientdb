package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already parsed SQL statement executors. It stores itself in the storage as a resource. It also
 * acts an an entry point for the SQL parser.
 *
 * @author Luigi Dell'Aquila
 */
public class OStatementCache {

  Map<String, OStatement> map;
  int                     mapSize;

  /**
   * @param size the size of the cache
   */
  public OStatementCache(int size) {
    this.mapSize = size;
    map = new LinkedHashMap<String, OStatement>(size) {
      protected boolean removeEldestEntry(final Map.Entry<String, OStatement> eldest) {
        return super.size() > mapSize;
      }
    };
  }

  /**
   * @param statement an SQL statement
   * @return true if the corresponding executor is present in the cache
   */
  public synchronized boolean contains(String statement) {
    return map.containsKey(statement);
  }

  /**
   * returns an already parsed SQL executor, taking it from the cache if it exists or creating a new one (parsing and then putting
   * it into the cache) if it doesn't
   *
   * @param statement the SQL statement
   * @param db        the current DB instance. If null, cache is ignored and a new executor is created through statement parsing
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
   * @param statement an SQL statement
   * @return the corresponding executor, taking it from the internal cache, if it exists
   */
  public synchronized OStatement get(String statement) {
    OStatement result = map.remove(statement);
    if (result == null) {
      result = parse(statement);
    }
    map.put(statement, result);
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
      final InputStream is = new ByteArrayInputStream(statement.getBytes());
      final OrientSql osql = new OrientSql(is);
      OStatement result = osql.parse();
      return result;
    } catch (ParseException e) {
      throwParsingException(e, statement);
    }
    return null;
  }

  protected static void throwParsingException(ParseException e, String statement) {
    throw new OCommandSQLParsingException(e, statement);
  }

}
