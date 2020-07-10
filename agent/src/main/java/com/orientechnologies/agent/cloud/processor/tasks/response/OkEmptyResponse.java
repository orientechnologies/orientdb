package com.orientechnologies.agent.cloud.processor.tasks.response;

import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** Created by Enrico Risa on 18/01/2018. */
public class OkEmptyResponse implements NodeOperationResponse {

  @Override
  public void write(DataOutput out) throws IOException {}

  @Override
  public void read(DataInput in) throws IOException {}
}
