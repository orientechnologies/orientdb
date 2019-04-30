package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class OStructuralSharedConfiguration {

  private Map<ONodeIdentity, OStructuralNodeConfiguration> knownNodes;
  private int                                              quorum;

  public void deserialize(DataInput input) throws IOException {
    this.quorum = input.readInt();
    int nKnowNodes = input.readInt();
    knownNodes = new HashMap<>();
    while (nKnowNodes-- > 0) {
      OStructuralNodeConfiguration node = new OStructuralNodeConfiguration();
      node.deserialize(input);
      knownNodes.put(node.getIdentity(), node);
    }
  }

  public void init(int quorum) {
    knownNodes = new HashMap<>();
    this.quorum = quorum;
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeInt(quorum);
    output.writeInt(knownNodes.size());
    for (OStructuralNodeConfiguration node : knownNodes.values()) {
      node.serialize(output);
    }
  }

  public void addNode(OStructuralNodeConfiguration node) {
    knownNodes.put(node.getIdentity(), node);
  }

  public Collection<OStructuralNodeConfiguration> listNodes() {
    return knownNodes.values();
  }

  public void distributeSerialize(DataOutput output) throws IOException {
    //For now just use the same of disc serialization but this will change in future
    serialize(output);
  }

  public void distributeDeserialize(DataInput input) throws IOException {
    //For now just use the same of disc serialization but this will change in future
    deserialize(input);
  }

  public OStructuralNodeConfiguration getNode(ONodeIdentity nodeIdentity) {
    return knownNodes.get(nodeIdentity);
  }

  public boolean existsNode(ONodeIdentity identity) {
    return knownNodes.containsKey(identity);
  }

  public boolean canAddNode(ONodeIdentity identity) {
    return knownNodes.size() < (quorum - 1) * 2;
  }
}
