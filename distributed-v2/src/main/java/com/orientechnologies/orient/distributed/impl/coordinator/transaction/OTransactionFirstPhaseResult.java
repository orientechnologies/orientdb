package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.TRANSACTION_FIRST_PHASE_RESPONSE;

import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OConcurrentModificationResult;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OExceptionResult;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OTransactionResult;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OUniqueKeyViolationResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OTransactionFirstPhaseResult implements ONodeResponse {

  public OTransactionFirstPhaseResult() {}

  public enum Type {
    SUCCESS,
    CONCURRENT_MODIFICATION_EXCEPTION,
    UNIQUE_KEY_VIOLATION,
    EXCEPTION
  }

  private Type type;
  private OTransactionResult resultMetadata;

  public OTransactionFirstPhaseResult(Type type, OTransactionResult resultMetadata) {
    this.type = type;
    this.resultMetadata = resultMetadata;
  }

  public Type getType() {
    return type;
  }

  public OTransactionResult getResultMetadata() {
    return resultMetadata;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    switch (type) {
      case SUCCESS:
        output.writeByte(1);
        break;
      case UNIQUE_KEY_VIOLATION:
        output.writeByte(2);
        break;
      case CONCURRENT_MODIFICATION_EXCEPTION:
        output.writeByte(3);
        break;
      case EXCEPTION:
        output.writeByte(4);
        break;
    }
    if (type != Type.SUCCESS) {
      this.resultMetadata.serialize(output);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    byte t = input.readByte();
    switch (t) {
      case 1:
        type = Type.SUCCESS;
        break;
      case 2:
        type = Type.UNIQUE_KEY_VIOLATION;
        resultMetadata = new OUniqueKeyViolationResult();
        break;
      case 3:
        type = Type.CONCURRENT_MODIFICATION_EXCEPTION;
        resultMetadata = new OConcurrentModificationResult();
        break;
      case 4:
        type = Type.EXCEPTION;
        resultMetadata = new OExceptionResult();
        break;
    }
    if (type != Type.SUCCESS) {
      resultMetadata.deserialize(input);
    }
  }

  @Override
  public int getResponseType() {
    return TRANSACTION_FIRST_PHASE_RESPONSE;
  }
}
