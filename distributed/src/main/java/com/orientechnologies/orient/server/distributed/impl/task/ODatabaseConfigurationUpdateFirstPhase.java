package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODatabaseConfigurationUpdateFirstPhase extends OAbstractRemoteTask {

  public static final int FACTORYID = 61;

  private ODocument configuration;

  public ODatabaseConfigurationUpdateFirstPhase() {}

  public ODatabaseConfigurationUpdateFirstPhase(ODocument configuration) {
    this.configuration = configuration;
  }

  @Override
  public String getName() {
    return "database_configuration_update_first_phase";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      OServer iServer,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database)
      throws Exception {

    return null;
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    super.toStream(out);
    byte[] bytes = ORecordSerializerNetworkDistributed.INSTANCE.toStream(this.configuration);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
    int lenght = in.readInt();
    byte[] bytes = new byte[lenght];
    in.readFully(bytes);
    configuration =
        (ODocument) ORecordSerializerNetworkDistributed.INSTANCE.fromStream(bytes, new ODocument());
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
