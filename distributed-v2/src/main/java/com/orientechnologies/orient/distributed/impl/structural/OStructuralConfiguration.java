package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.server.OSystemDatabase;

import java.io.*;

public class OStructuralConfiguration {

  private static final String                         CLUSTER_NAME = "__DISTRIBUTED_CONFIG__";
  private              OSystemDatabase                systemDatabase;
  private              ONodeIdentity                  currentNodeIdentity;
  private              OStructuralSharedConfiguration sharedConfiguration;

  public OStructuralConfiguration(OSystemDatabase systemDatabase, OrientDBInternal context) {
    this.systemDatabase = systemDatabase;
    load();
  }

  private synchronized void load() {
    systemDatabase.executeInDBScope((session) -> {
      try {
        if (!session.existsCluster(CLUSTER_NAME)) {
          session.addCluster(CLUSTER_NAME);
        }
        ORecordIteratorCluster<ORecord> config_record = session.browseCluster(CLUSTER_NAME);
        if (config_record.hasNext()) {
          ORecordBytes record = (ORecordBytes) config_record.next();

          this.deserialize(new DataInputStream(new ByteArrayInputStream(record.toStream())));
        } else {
          this.init();

          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          this.serialize(new DataOutputStream(buffer));
          session.save(new ORecordBytes(buffer.toByteArray()), CLUSTER_NAME);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    });
  }

  public void serialize(DataOutput output) throws IOException {
    this.currentNodeIdentity.serialize(output);
    this.sharedConfiguration.serialize(output);
  }

  private void init() {
    this.currentNodeIdentity = ONodeIdentity.generate();
    this.sharedConfiguration = new OStructuralSharedConfiguration();
    this.sharedConfiguration.init();
  }

  public void deserialize(DataInput input) throws IOException {
    this.currentNodeIdentity = new ONodeIdentity();
    this.currentNodeIdentity.deserialize(input);
    sharedConfiguration = new OStructuralSharedConfiguration();
    sharedConfiguration.deserialize(input);
  }

  private void save() {

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

  public OStructuralSharedConfiguration getSharedConfiguration() {
    return sharedConfiguration;
  }
}
