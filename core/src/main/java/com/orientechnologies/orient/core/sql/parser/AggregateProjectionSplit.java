package com.orientechnologies.orient.core.sql.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used by the query planner to split projections in three parts:
 *
 * <ul>
 *   <li>pre-aggregate projections
 *   <li>aggregate projections
 *   <li>post-aggregate projections
 * </ul>
 *
 * An example: <code>
 *   select max(a + b) + (max(b + c * 2) + 1 + 2) * 3 as foo, max(d) + max(e), f from " + className
 * </code> will become <code>
 *
 *   a + b AS _$$$OALIAS$$_1, b + c * 2 AS _$$$OALIAS$$_3, d AS _$$$OALIAS$$_5, e AS _$$$OALIAS$$_7, f
 *
 *   max(_$$$OALIAS$$_1) AS _$$$OALIAS$$_0, max(_$$$OALIAS$$_3) AS _$$$OALIAS$$_2, max(_$$$OALIAS$$_5) AS _$$$OALIAS$$_4, max(_$$$OALIAS$$_7) AS _$$$OALIAS$$_6, f
 *
 *   _$$$OALIAS$$_0 + (_$$$OALIAS$$_2 + 1 + 2) * 3 AS `foo`, _$$$OALIAS$$_4 + _$$$OALIAS$$_6 AS `max(d) + max(e)`, f
 * </code>
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class AggregateProjectionSplit {

  protected static final String GENERATED_ALIAS_PREFIX = "_$$$OALIAS$$_";
  protected int nextAliasId = 0;

  protected List<OProjectionItem> preAggregate = new ArrayList<>();
  protected List<OProjectionItem> aggregate = new ArrayList<>();

  public OIdentifier getNextAlias() {
    OIdentifier result = new OIdentifier(GENERATED_ALIAS_PREFIX + (nextAliasId++));
    result.internalAlias = true;
    return result;
  }

  public List<OProjectionItem> getPreAggregate() {
    return preAggregate;
  }

  public void setPreAggregate(List<OProjectionItem> preAggregate) {
    this.preAggregate = preAggregate;
  }

  public List<OProjectionItem> getAggregate() {
    return aggregate;
  }

  public void setAggregate(List<OProjectionItem> aggregate) {
    this.aggregate = aggregate;
  }

  /** clean the content, but NOT the counter! */
  public void reset() {
    this.preAggregate.clear();
    this.aggregate.clear();
  }
}
