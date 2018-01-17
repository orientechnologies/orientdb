package com.orientechnologies.agent.cloud.processor.tasks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.server.distributed.operation.NodeOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Enrico Risa on 17/01/2018.
 */
public abstract class AbstractRPCTask<T> implements NodeOperation {

  ObjectMapper mapper = new ObjectMapper();

  protected abstract T getPayload();

  protected abstract void setPayload(T payload);

  protected abstract Class<T> getPayloadType();

  @Override
  public void write(DataOutput out) throws IOException {

    String msg = mapper.writeValueAsString(getPayload());
    out.writeUTF(msg);

  }

  @Override
  public void read(DataInput in) throws IOException {

    String msg = in.readUTF();
    T payload = mapper.readValue(msg, getPayloadType());
    setPayload(payload);
  }
}
