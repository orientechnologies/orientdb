package com.orientechnologies.orient.core.sql.parser;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is used by the query planner to extract subqueries and move them to LET clause <br>
 * An example: <br>
 * <br>
 * <code>
 * select from foo where name in (select name from bar)
 * </code> <br>
 * <br>
 * will become <br>
 * <br>
 * <code>
 * select from foo<br>
 * let $$$SUBQUERY$$_0 = (select name from bar)<br>
 * where name in $$$SUBQUERY$$_0
 * </code> <br>
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class SubQueryCollector {

  protected static final String GENERATED_ALIAS_PREFIX = "$$$SUBQUERY$$_";
  protected int nextAliasId = 0;

  protected Map<OIdentifier, OStatement> subQueries = new LinkedHashMap<>();

  protected OIdentifier getNextAlias() {
    OIdentifier result = new OIdentifier(GENERATED_ALIAS_PREFIX + (nextAliasId++));
    result.internalAlias = true;
    return result;
  }

  /** clean the content, but NOT the counter! */
  public void reset() {
    this.subQueries.clear();
  }

  public OIdentifier addStatement(OIdentifier alias, OStatement stm) {
    subQueries.put(alias, stm);
    return alias;
  }

  public OIdentifier addStatement(OStatement stm) {
    OIdentifier alias = getNextAlias();
    return addStatement(alias, stm);
  }

  public Map<OIdentifier, OStatement> getSubQueries() {
    return subQueries;
  }
}
