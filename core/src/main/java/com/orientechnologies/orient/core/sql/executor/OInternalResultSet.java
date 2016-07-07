package com.orientechnologies.orient.core.sql.executor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class OInternalResultSet implements OTodoResultSet {
  List<OResult> content = new ArrayList<>();
  int           next    = 0;

  @Override public boolean hasNext() {
    return content.size() > next;
  }

  @Override public OResult next() {
    return content.get(next++);
  }

  @Override public void close() {
    this.content.clear();
  }

  public void add(OResult nextResult) {
    content.add(nextResult);
  }

}
