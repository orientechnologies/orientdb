package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.DistributedQueryContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by luigidellaquila on 28/06/17.
 */
public class OFetchQueryPageTask extends OAbstractRemoteTask {

  public static final int FACTORYID = 41;

  private String queryId;

  public OFetchQueryPageTask(String queryId) {
    this.queryId = queryId;
  }

  public OFetchQueryPageTask() {
  }

  @Override
  public String getName() {
    return "OFetchQueryPageTask";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {

    DistributedQueryContext ctx = ((OSharedContextEmbedded) database.getSharedContext()).getActiveDistributedQueries().get(queryId);
    if (ctx == null) {
      throw new ODistributedException("Invalid query ID: " + queryId);
    }
    return ctx.fetchNextPage();
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    char[] chars = queryId.toCharArray();
    int length = chars.length;
    out.write(length);
    for (char aChar : chars) {
      out.writeChar(aChar);
    }
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    int length = in.readInt();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      builder.append(in.readChar());
    }
    this.queryId = builder.toString();
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}