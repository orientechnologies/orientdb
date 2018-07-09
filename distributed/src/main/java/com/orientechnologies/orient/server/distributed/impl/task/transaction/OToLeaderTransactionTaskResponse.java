package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.client.remote.message.OCommit37Response;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OStreamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class OToLeaderTransactionTaskResponse implements OStreamable {

  public static class OCreatedRecordResponse {
    private final ORID currentRid;
    private final ORID createdRid;
    private final int  version;

    public OCreatedRecordResponse(ORID currentRid, ORID createdRid, int version) {
      this.currentRid = currentRid;
      this.createdRid = createdRid;
      this.version = version;
    }

    public ORID getCreatedRid() {
      return createdRid;
    }

    public ORID getCurrentRid() {
      return currentRid;
    }

    public int getVersion() {
      return version;
    }
  }

  public static class OUpdatedRecordResponse {
    private final ORID rid;
    private final int  version;

    public OUpdatedRecordResponse(ORID rid, int version) {
      this.rid = rid;
      this.version = version;
    }

    public ORID getRid() {
      return rid;
    }

    public int getVersion() {
      return version;
    }
  }

  public static class ODeletedRecordResponse {
    private final ORID rid;

    public ODeletedRecordResponse(ORID rid) {
      this.rid = rid;
    }

    public ORID getRid() {
      return rid;
    }

  }

  private List<OCreatedRecordResponse> created;
  private List<OUpdatedRecordResponse> updated;
  private List<ODeletedRecordResponse> deleted;

  public OToLeaderTransactionTaskResponse() {
  }

  public OToLeaderTransactionTaskResponse(Map<ORID, ORecord> createdRecords, Map<ORID, ORecord> updatedRecords,
      Set<ORID> deletedRecord) {
    created = new ArrayList<>();
    for (Map.Entry<ORID, ORecord> create : createdRecords.entrySet()) {
      created.add(new OCreatedRecordResponse(create.getKey(), create.getValue().getIdentity(), create.getValue().getVersion()));
    }
    updated = new ArrayList<>();
    for (Map.Entry<ORID, ORecord> update : updatedRecords.entrySet()) {
      updated.add(new OUpdatedRecordResponse(update.getKey(), update.getValue().getVersion()));
    }
    deleted = deletedRecord.stream().map((x) -> new ODeletedRecordResponse(x)).collect(Collectors.toList());
  }

  @Override
  public void toStream(DataOutput out) throws IOException {

    out.writeInt(created.size());
    for (OCreatedRecordResponse createdRecord : created) {
      out.writeShort(createdRecord.currentRid.getClusterId());
      out.writeLong(createdRecord.currentRid.getClusterPosition());
      out.writeShort(createdRecord.createdRid.getClusterId());
      out.writeLong(createdRecord.createdRid.getClusterPosition());
      out.writeInt(createdRecord.version);
    }

    out.writeInt(updated.size());
    for (OUpdatedRecordResponse updatedRecord : updated) {
      out.writeShort(updatedRecord.rid.getClusterId());
      out.writeLong(updatedRecord.rid.getClusterPosition());

      out.writeInt(updatedRecord.version);
    }

    out.writeInt(deleted.size());
    for (ODeletedRecordResponse deleteRecord : deleted) {
      out.writeShort(deleteRecord.rid.getClusterId());
      out.writeLong(deleteRecord.rid.getClusterPosition());
    }
  }

  @Override
  public void fromStream(DataInput in) throws IOException {

    final int createdRecords = in.readInt();
    created = new ArrayList<>(createdRecords);
    ORecordId currentRid;
    ORecordId createdRid;
    for (int i = 0; i < createdRecords; i++) {
      currentRid = new ORecordId();
      currentRid.fromStream(in);
      createdRid = new ORecordId();
      createdRid.fromStream(in);
      int version = in.readInt();
      created.add(new OCreatedRecordResponse(currentRid, createdRid, version));
    }
    final int updatedRecords = in.readInt();
    updated = new ArrayList<>(updatedRecords);

    for (int i = 0; i < updatedRecords; ++i) {
      ORecordId rid = new ORecordId();
      rid.fromStream(in);
      int version = in.readInt();
      updated.add(new OUpdatedRecordResponse(rid, version));
    }

    final int deletedRecords = in.readInt();
    deleted = new ArrayList<>(deletedRecords);

    for (int i = 0; i < deletedRecords; ++i) {
      ORecordId rid = new ORecordId();
      rid.fromStream(in);
      deleted.add(new ODeletedRecordResponse(rid));
    }
  }

  public List<OCreatedRecordResponse> getCreated() {
    return created;
  }

  public List<OUpdatedRecordResponse> getUpdated() {
    return updated;
  }

  public List<ODeletedRecordResponse> getDeleted() {
    return deleted;
  }
}
