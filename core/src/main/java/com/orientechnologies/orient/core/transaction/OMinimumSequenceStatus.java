package com.orientechnologies.orient.core.transaction;

import java.util.HashMap;
import java.util.Map;

/** This Compute the minimum sequence of a group of node sequences.
 *
 * It used to figure out what log entries can be removed from the log because all
 * the nodes are updated with them.
 *
 */
public class OMinimumSequenceStatus {

  private Map<ONodeId, long[]> nodesSequentials;
  private OSequenceNodes[] minimumSequentials;

  public OMinimumSequenceStatus(int size) {
    this.minimumSequentials = new OSequenceNodes[size];
    for (int i = 0; i < size; i++) {
      this.minimumSequentials[i] = new OSequenceNodes(0);
    }
    nodesSequentials = new HashMap<>();
  }

  public OMinimumSequenceStatus(long[] current) {
    this.minimumSequentials = new OSequenceNodes[current.length];
    for (int i = 0; i < current.length; i++) {
      this.minimumSequentials[i] = new OSequenceNodes(current[i]);
    }
    nodesSequentials = new HashMap<>();
  }

  public synchronized void updateNode(ONodeId nodeID, long[] sequences) {
    assert sequences.length == this.minimumSequentials.length;
    for (int i = 0; i < sequences.length; i++) {
      this.minimumSequentials[i].nodeUpdate(nodeID, sequences[i]);
    }
    nodesSequentials.put(nodeID, sequences);
  }

  public synchronized long[] getMinimumSequence() {
    long[] seqs = new long[this.minimumSequentials.length];
    for (int i = 0; i < minimumSequentials.length; i++) {
      seqs[i] = minimumSequentials[i].getMinimum();
    }
    return seqs;
  }
}
