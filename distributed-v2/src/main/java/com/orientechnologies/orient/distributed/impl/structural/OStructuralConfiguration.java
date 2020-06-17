package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.server.OSystemDatabase;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class OStructuralConfiguration {

  private static final String CLUSTER_NAME = "__DISTRIBUTED_CONFIG__";
  private OSystemDatabase systemDatabase;
  private ONodeIdentity currentNodeIdentity;
  private OLogId lastUpdateId;
  private OStructuralSharedConfiguration sharedConfiguration;

  public OStructuralConfiguration(OSystemDatabase systemDatabase, OrientDBDistributed context) {
    this.systemDatabase = systemDatabase;
    load(context.getNodeConfig().getNodeName(), context.getNodeConfig().getQuorum());
  }

  private synchronized void load(String nodeName, int quorum) {
    systemDatabase.executeInDBScope(
        (session) -> {
          try {
            if (!session.existsCluster(CLUSTER_NAME)) {
              session.addCluster(CLUSTER_NAME);
            }
            ORecordIteratorCluster<ORecord> configRecord = session.browseCluster(CLUSTER_NAME);
            if (configRecord.hasNext()) {
              ORecordBytes record = (ORecordBytes) configRecord.next();

              this.discDeserialize(
                  new DataInputStream(new ByteArrayInputStream(record.toStream())));
            } else {
              this.init(nodeName, quorum);

              ByteArrayOutputStream buffer = new ByteArrayOutputStream();
              this.discSerialize(new DataOutputStream(buffer));
              session.save(new ORecordBytes(buffer.toByteArray()), CLUSTER_NAME);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
          return null;
        });
  }

  protected void discSerialize(DataOutput output) throws IOException {
    OLogId.serialize(lastUpdateId, output);
    this.currentNodeIdentity.serialize(output);
    this.sharedConfiguration.serialize(output);
  }

  private void init(String nodeName, int quorum) {
    this.currentNodeIdentity = ONodeIdentity.generate(nodeName);
    this.sharedConfiguration = new OStructuralSharedConfiguration();
    this.sharedConfiguration.init(quorum);
  }

  protected void discDeserialize(DataInput input) throws IOException {
    this.lastUpdateId = OLogId.deserialize(input);
    this.currentNodeIdentity = new ONodeIdentity();
    this.currentNodeIdentity.deserialize(input);
    sharedConfiguration = new OStructuralSharedConfiguration();
    sharedConfiguration.deserialize(input);
  }

  public synchronized void saveFromNetwork(OLogId id) {
    this.lastUpdateId = id;
    saveInternal();
  }

  public void save() {
    saveInternal();
  }

  public synchronized void saveInternal() {
    systemDatabase.executeInDBScope(
        (session) -> {
          try {
            assert session.existsCluster(CLUSTER_NAME);
            ORecordIteratorCluster<ORecord> configRecord = session.browseCluster(CLUSTER_NAME);
            if (configRecord.hasNext()) {
              ORecordBytes record = (ORecordBytes) configRecord.next();
              ByteArrayOutputStream buffer = new ByteArrayOutputStream();
              this.discSerialize(new DataOutputStream(buffer));
              record.setDirty();
              record.fromStream(buffer.toByteArray());
              session.save(record);
            } else {
              throw new ODatabaseException("Missing configuration record");
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
          return null;
        });
  }

  public OStructuralNodeConfiguration getConfiguration(ONodeIdentity nodeId) {
    return null;
  }

  OStructuralNodeConfiguration getCurrentConfiguration() {
    return getConfiguration(getCurrentNodeIdentity());
  }

  public ONodeIdentity getCurrentNodeIdentity() {
    return currentNodeIdentity;
  }

  public OReadStructuralSharedConfiguration readSharedConfiguration() {
    return sharedConfiguration;
  }

  public OStructuralSharedConfiguration modifySharedConfiguration() {
    try {
      return sharedConfiguration.clone();
    } catch (CloneNotSupportedException e) {
      throw OException.wrapException(new ODatabaseException("Cloning error"), e);
    }
  }

  public synchronized void receiveSharedConfiguration(
      OLogId lastId, OReadStructuralSharedConfiguration sharedConfiguration) {
    this.lastUpdateId = lastId;
    this.sharedConfiguration = (OStructuralSharedConfiguration) sharedConfiguration;
    this.save();
  }

  public OLogId getLastUpdateId() {
    return lastUpdateId;
  }

  public synchronized void update(OStructuralSharedConfiguration config) {
    this.sharedConfiguration = config;
    save();
  }
}
