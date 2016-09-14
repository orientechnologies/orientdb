package com.orientechnologies.orient.core.sql.parser;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used by the query planner to extract subqueries and move them to LET clause
 * <br>
 * An example:
 * <br>
 * <br>
 * <code>
 * select from foo where name in (select name from bar)
 * </code>
 * <br><br>
 * will become
 * <br><br>
 * <code>
 * select from foo<br>
 * let _$$$SUBQUERY$$_0 = (select name from bar)<br>
 * where name in _$$$SUBQUERY$$_0
 * </code>
 * <br>
 *
 * @author Luigi Dell'Aquila
 */
public class SubQueryCollector {

  protected static final String GENERATED_ALIAS_PREFIX = "_$$$SUBQUERY$$_";
  protected              int    nextAliasId            = 0;

  protected Map<OIdentifier, OStatement> subQueries = new HashMap<>();

  protected OIdentifier getNextAlias() {
    OIdentifier result = new OIdentifier(-1);
    result.setStringValue(GENERATED_ALIAS_PREFIX + (nextAliasId++));
    result.internalAlias = true;
    return result;
  }

  /**
   * clean the content, but NOT the counter!
   */
  public void reset() {
    this.subQueries.clear();
  }

  public OIdentifier addStatement(OStatement stm) {
    OIdentifier alias = getNextAlias();
    subQueries.put(alias, stm);
    return alias;
  }

  public Map<OIdentifier, OStatement> getSubQueries() {
    return subQueries;
  }
}
