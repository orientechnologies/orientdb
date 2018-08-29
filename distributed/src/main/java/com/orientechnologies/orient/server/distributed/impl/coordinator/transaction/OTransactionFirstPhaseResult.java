package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeResponse;

import java.util.List;

public class OTransactionFirstPhaseResult implements ONodeResponse {

  public OTransactionFirstPhaseResult(Type type, Object resultMetadata) {
    this.type = type;
    this.resultMetadata = resultMetadata;
  }

  public enum Type {
    SUCCESS, CONCURRENT_MODIFICATION_EXCEPTION, UNIQUE_KEY_VIOLATION, EXCEPTION
  }

  private Type   type;
  private Object resultMetadata;

  public static class Success {
    private List<ORecordId> allocatedIds;

    public Success(List<ORecordId> allocatedIds) {
      this.allocatedIds = allocatedIds;
    }

    public List<ORecordId> getAllocatedIds() {
      return allocatedIds;
    }
  }

  public static class ConcurrentModification {
    private final ORecordId recordId;
    private final int       updateVersion;
    private final int       persistentVersion;

    public ConcurrentModification(ORecordId recordId, int updateVersion, int persistentVersion) {
      this.recordId = recordId;
      this.updateVersion = updateVersion;
      this.persistentVersion = persistentVersion;
    }

    public ORecordId getRecordId() {
      return recordId;
    }

    public int getUpdateVersion() {
      return updateVersion;
    }

    public int getPersistentVersion() {
      return persistentVersion;
    }
  }

  public static class UniqueKeyViolation {
    private String    keyStringified;
    private ORecordId recordRequesting;
    private ORecordId recordOwner;
    private String    indexName;

    public UniqueKeyViolation(String keyStringified, ORecordId recordRequesting, ORecordId recordOwner, String indexName) {
      this.keyStringified = keyStringified;
      this.recordRequesting = recordRequesting;
      this.recordOwner = recordOwner;
      this.indexName = indexName;
    }

    public String getKeyStringified() {
      return keyStringified;
    }

    public ORecordId getRecordRequesting() {
      return recordRequesting;
    }

    public ORecordId getRecordOwner() {
      return recordOwner;
    }

    public String getIndexName() {
      return indexName;
    }
  }

  public Type getType() {
    return type;
  }

  public Object getResultMetadata() {
    return resultMetadata;
  }
}
