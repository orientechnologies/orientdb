package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author Luigi Dell'Aquila
 */
public class OStatementCache {

  Map<String, OStatement> map;
  int                     mapSize;

  public OStatementCache(int size) {
    this.mapSize = size;
    map = Collections.synchronizedMap(new LinkedHashMap<String, OStatement>(size) {
      protected boolean removeEldestEntry(final Map.Entry<String, OStatement> eldest) {
        return super.size() > mapSize;
      }
    });
  }

  public boolean contains(String statement) {
    return map.containsKey(statement);
  }

  public static OStatement get(String statement, ODatabaseDocumentInternal db) {
    if (db == null) {
      return parse(statement);
    }

    OStatementCache resource = db.getStorage().getResource(OStatementCache.class.getSimpleName(), new Callable<OStatementCache>() {
      @Override
      public OStatementCache call() throws Exception {
        return new OStatementCache(OGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger());
      }
    });
    return resource.get(statement);
  }

  public synchronized OStatement get(String statement) {
    OStatement result = map.remove(statement);
    if (result == null) {
      result = parse(statement);
    }
    map.put(statement, result);
    return result;
  }

  protected static OStatement parse(String statement) throws OCommandSQLParsingException {
    try {
      final InputStream is = new ByteArrayInputStream(statement.getBytes());
      final OrientSql osql = new OrientSql(is);
      OStatement result = osql.parse();
      return result;
    } catch (ParseException e) {
      throwParsingException(e.getMessage());
    }
    return null;
  }

  protected static void throwParsingException(final String iText) {
    throw new OCommandSQLParsingException(iText);
  }

}
