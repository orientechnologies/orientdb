package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.index.OIndex;
import java.util.Iterator;

public class OrientIndexQuery {
  public final Iterator<Object> values;
  public final OIndex index;

  public OrientIndexQuery(OIndex index, Iterator<Object> values) {
    this.index = index;
    this.values = values;
  }

  public String toString() {
    return "OrientIndexQuery(index=" + index + ")";
  }
}
