package com.orientechnologies.orient.core.sql.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 15/07/16.
 */
public class AggregateProjectionSplit {

  protected static final String GENERATED_ALIAS_PREFIX = "_$$$OALIAS$$_";
  protected              int    nextAliasId            = 0;

  protected List<OProjectionItem> preAggregate = new ArrayList<>();
  protected List<OProjectionItem>   aggregate    = new ArrayList<>();


  public OIdentifier getNextAlias() {
    OIdentifier result = new OIdentifier(-1);
    result.setStringValue(GENERATED_ALIAS_PREFIX + (nextAliasId++));
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


  public void reset() {
    this.preAggregate.clear();
    this.aggregate.clear();
  }
}
