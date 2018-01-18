package com.orientechnologies.agent.cloud.processor.tasks;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.operation.NodeOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Enrico Risa on 17/01/2018.
 */
public abstract class AbstractDocumentTask implements NodeOperation {

  protected ODocument payload;

  public AbstractDocumentTask() {

  }

  public AbstractDocumentTask(ODocument payload) {
    this.payload = payload;
  }

  @Override
  public void write(DataOutput out) throws IOException {

    out.writeUTF(payload.toJSON());

  }

  @Override
  public void read(DataInput in) throws IOException {

    String msg = in.readUTF();
    this.payload = new ODocument().fromJSON(msg);
  }
}
