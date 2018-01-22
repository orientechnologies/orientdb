package com.orientechnologies.agent.cloud.processor.tasks;

import com.orientechnologies.orientdb.cloud.protocol.ServerThreadDump;

public class ThreadDumpTaskResponse extends AbstractRPCTaskResponse<ServerThreadDump> {

  public ThreadDumpTaskResponse() {
  }

  public ThreadDumpTaskResponse(ServerThreadDump threadDump) {
    super(threadDump);
  }

  @Override
  protected Class<ServerThreadDump> getPayloadType() {
    return ServerThreadDump.class;
  }

}
