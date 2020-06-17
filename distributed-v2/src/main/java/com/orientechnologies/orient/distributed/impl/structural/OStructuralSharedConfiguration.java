package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OStructuralSharedConfiguration
    implements OReadStructuralSharedConfiguration, Cloneable {

  private List<String> databases;
  private Map<ONodeIdentity, OStructuralNodeConfiguration> knownNodes;
  private int quorum;

  public void init(int quorum) {
    this.databases = new ArrayList<>();
    this.knownNodes = new HashMap<>();
    this.quorum = quorum;
  }

  public void deserialize(DataInput input) throws IOException {
    this.quorum = input.readInt();
    int nKnowNodes = input.readInt();
    knownNodes = new HashMap<>();
    while (nKnowNodes-- > 0) {
      OStructuralNodeConfiguration node = new OStructuralNodeConfiguration();
      node.deserialize(input);
      knownNodes.put(node.getIdentity(), node);
    }
    int nDatabases = input.readInt();
    databases = new ArrayList<>();
    while (nDatabases-- > 0) {
      databases.add(input.readUTF());
    }
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeInt(quorum);
    output.writeInt(knownNodes.size());
    for (OStructuralNodeConfiguration node : knownNodes.values()) {
      node.serialize(output);
    }
    output.writeInt(databases.size());
    for (String database : databases) {
      output.writeUTF(database);
    }
  }

  @Override
  public void networkSerialize(DataOutput output) throws IOException {
    // TODO: Make sure that network become independent to the disc.
    serialize(output);
  }

  @Override
  public void networkDeserialize(DataInput input) throws IOException {
    // TODO: Make sure that network become independent to the disc.
    deserialize(input);
  }

  public void addNode(OStructuralNodeConfiguration node) {
    knownNodes.put(node.getIdentity(), node);
  }

  public Collection<OStructuralNodeConfiguration> listNodes() {
    return knownNodes.values();
  }

  public void distributeSerialize(DataOutput output) throws IOException {
    // For now just use the same of disc serialization but this will change in future
    serialize(output);
  }

  public void distributeDeserialize(DataInput input) throws IOException {
    // For now just use the same of disc serialization but this will change in future
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

  public boolean existsDatabase(String database) {
    return databases.contains(database);
  }

  public void addDatabase(String database) {
    databases.add(database);
  }

  public int getQuorum() {
    return quorum;
  }

  @Override
  public OStructuralSharedConfiguration clone() throws CloneNotSupportedException {
    return (OStructuralSharedConfiguration) super.clone();
  }

  public void removeDatabase(String database) {
    databases.remove(database);
  }
}
