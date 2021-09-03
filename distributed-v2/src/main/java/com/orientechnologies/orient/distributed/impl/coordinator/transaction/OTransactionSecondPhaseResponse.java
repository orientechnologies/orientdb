package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.TRANSACTION_SECOND_PHASE_RESPONSE;

import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OTransactionSecondPhaseResponse implements ONodeResponse {
  private boolean success;
  private List<OCreatedRecordResponse> createdRecords;
  private List<OUpdatedRecordResponse> updatedRecords;
  private List<ODeletedRecordResponse> deletedRecords;

  public OTransactionSecondPhaseResponse() {}

  public OTransactionSecondPhaseResponse(
      boolean success,
      List<OCreatedRecordResponse> createdRecords,
      List<OUpdatedRecordResponse> updatedRecords,
      List<ODeletedRecordResponse> deletedRecords) {
    this.success = success;
    this.createdRecords = createdRecords;
    this.updatedRecords = updatedRecords;
    this.deletedRecords = deletedRecords;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeBoolean(success);
    output.writeInt(createdRecords.size());
    for (OCreatedRecordResponse createdRecord : createdRecords) {
      createdRecord.serialize(output);
    }
    output.writeInt(updatedRecords.size());
    for (OUpdatedRecordResponse updatedRecord : updatedRecords) {
      updatedRecord.serialize(output);
    }
    output.writeInt(deletedRecords.size());
    for (ODeletedRecordResponse deletedRecord : deletedRecords) {
      deletedRecord.serialize(output);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    success = input.readBoolean();
    int createSize = input.readInt();
    createdRecords = new ArrayList<>(createSize);
    while (createSize-- > 0) {
      OCreatedRecordResponse createResponse = new OCreatedRecordResponse();
      createResponse.deserialize(input);
      createdRecords.add(createResponse);
    }
    int updateSize = input.readInt();
    updatedRecords = new ArrayList<>(updateSize);
    while (updateSize-- > 0) {
      OUpdatedRecordResponse updateRecord = new OUpdatedRecordResponse();
      updateRecord.deserialize(input);
      updatedRecords.add(updateRecord);
    }
    int deleteSize = input.readInt();
    deletedRecords = new ArrayList<>(deleteSize);
    while (deleteSize-- > 0) {
      ODeletedRecordResponse deletedResponse = new ODeletedRecordResponse();
      deletedResponse.deserialize(input);
      deletedRecords.add(deletedResponse);
    }
  }

  @Override
  public int getResponseType() {
    return TRANSACTION_SECOND_PHASE_RESPONSE;
  }

  public boolean isSuccess() {
    return success;
  }

  public List<OCreatedRecordResponse> getCreatedRecords() {
    return createdRecords;
  }

  public List<ODeletedRecordResponse> getDeletedRecords() {
    return deletedRecords;
  }

  public List<OUpdatedRecordResponse> getUpdatedRecords() {
    return updatedRecords;
  }
}
