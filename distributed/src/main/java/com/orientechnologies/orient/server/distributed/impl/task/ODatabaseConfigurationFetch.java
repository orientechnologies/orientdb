package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODatabaseConfigurationFetch extends OAbstractRemoteTask {

  public static final int FACTORYID = 63;

  private String database;

  public ODatabaseConfigurationFetch() {}

  public ODatabaseConfigurationFetch(String database) {}

  @Override
  public String getName() {
    return "database_configuration_fetch";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      OServer iServer,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database)
      throws Exception {
    // TODO: use an API that do not create the configuration in this case
    ODistributedConfiguration config =
        ((ODistributedDatabaseImpl) iManager.getDatabase(this.database))
            .getExistingDatabaseConfiguration();
    if (config != null) {
      return config.getDocument();
    }
    return null;
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    super.toStream(out);
    out.writeUTF(database);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
    this.database = in.readUTF();
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
