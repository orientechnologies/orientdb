package com.orientechnologies.orient.server.hazelcast.sharding.distributed;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public interface ODHTNode {
  public long getNodeId();

  public long getSuccessor();

  public Long getPredecessor();

  public void notify(long node);

  public boolean join(long node);

  public long findSuccessor(long id);

  public void notifyMigrationEnd(long nodeId);

  public void requestMigration(long requesterId);

  OPhysicalPosition createRecord(String storageName, ORecordId iRecordId, byte[] iContent, ORecordVersion iRecordVersion,
      byte iRecordType);

  ORawBuffer readRecord(String storageName, ORID iRid);

  ORecordVersion updateRecord(String storageName, ORecordId iRecordId, byte[] iContent, ORecordVersion iVersion, byte iRecordType);

  boolean deleteRecord(String storageName, ORecordId iRecordId, ORecordVersion iVersion);

  Object command(String storageName, OCommandRequestText request, boolean serializeResult);

  boolean isLocal();
}
