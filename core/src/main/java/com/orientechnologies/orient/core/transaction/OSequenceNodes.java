package com.orientechnologies.orient.core.transaction;

import java.util.HashMap;
import java.util.Map;

public class OSequenceNodes {

  private long minimum;
  private Map<ONodeId, Long> minimumForNodes;

  public OSequenceNodes(long currentLast) {
    this.minimum = currentLast;
    this.minimumForNodes = new HashMap<>();
  }

  public void nodeUpdate(ONodeId nodeId, long newValue) {
    var old = minimumForNodes.put(nodeId, newValue);
    if (old == null || old == minimum) {
      var newMinimum = Long.MAX_VALUE;
      for (var cur : minimumForNodes.values()) {
        if (newMinimum > cur) {
          newMinimum = cur;
        }
      }
      this.minimum = newMinimum;
    }
  }

  public long getMinimum() {
    return minimum;
  }
}
