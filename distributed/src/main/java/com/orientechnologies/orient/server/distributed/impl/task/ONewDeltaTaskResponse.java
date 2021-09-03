package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.serialization.OStreamable;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ONewDeltaTaskResponse implements OStreamable {

  public ONewDeltaTaskResponse() {}

  public ONewDeltaTaskResponse(ResponseType responseType) {
    this.responseType = responseType;
  }

  public ONewDeltaTaskResponse(ODistributedDatabaseChunk chunk) {
    this.responseType = ResponseType.CHUNK;
    this.chunk = Optional.of(chunk);
  }

  public enum ResponseType {
    CHUNK((byte) 1),
    FULL_SYNC((byte) 2),
    PARTIAL_CHUNK((byte) 3),
    NO_CHANGES((byte) 4);
    private byte value;

    ResponseType(byte b) {
      value = b;
    }

    public byte getValue() {
      return value;
    }

    static ResponseType fromValue(byte b) {
      switch (b) {
        case 1:
          return ResponseType.CHUNK;
        case 2:
          return ResponseType.FULL_SYNC;
        case 3:
          return ResponseType.PARTIAL_CHUNK;
        case 4:
          return ResponseType.NO_CHANGES;
      }
      return null;
    }
  }

  private ResponseType responseType;
  private Optional<ODistributedDatabaseChunk> chunk;

  @Override
  public void toStream(DataOutput out) throws IOException {
    out.writeByte(responseType.getValue());
    if (responseType == ResponseType.CHUNK) {
      chunk.get().toStream(out);
    }
  }

  @Override
  public void fromStream(DataInput in) throws IOException {
    responseType = ResponseType.fromValue(in.readByte());
    if (responseType == ResponseType.CHUNK) {
      ODistributedDatabaseChunk c = new ODistributedDatabaseChunk();
      c.fromStream(in);
      this.chunk = Optional.of(c);
    }
  }

  public ResponseType getResponseType() {
    return responseType;
  }

  public Optional<ODistributedDatabaseChunk> getChunk() {
    return chunk;
  }
}
