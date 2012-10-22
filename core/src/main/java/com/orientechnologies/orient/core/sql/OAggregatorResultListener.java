package com.orientechnologies.orient.core.sql;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Stub for receiving result set on the node that initiated query
 * 
 * @author gman
 * @since 21.10.12 18:37
 */
public class OAggregatorResultListener implements OCommandResultListener {

  private final List<OIdentifiable> result = new ArrayList<OIdentifiable>();

  @Override
  public boolean result(Object iRecord) {
    result.add((OIdentifiable) iRecord);
    return false;
  }

  public List<OIdentifiable> getResult() {
    return result;
  }
}
