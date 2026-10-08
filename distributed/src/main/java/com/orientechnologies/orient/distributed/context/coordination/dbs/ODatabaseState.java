package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum ODatabaseState {
  NotAvailable((byte) 1),
  Online((byte) 2),
  Offline((byte) 3);

  private byte stableId;

  private ODatabaseState(byte stableId) {
    this.stableId = stableId;
  }

  public static ODatabaseState readNetwork(DataInput input) throws IOException {
    byte id = input.readByte();
    return ODatabaseState.fromStableId(id);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeByte(this.stableId);
  }

  public static ODatabaseState from(ODistributedServerManager.DB_STATUS status) {
    switch (status) {
      case ONLINE:
        return ODatabaseState.Online;
      case OFFLINE:
        return ODatabaseState.Offline;
      case NOT_AVAILABLE:
        return ODatabaseState.NotAvailable;
    }
    return null;
  }

  public static ODatabaseState fromLegacyString(String legacy) {
    if ("online".equalsIgnoreCase(legacy)) return ODatabaseState.Online;
    else if ("offline".equalsIgnoreCase(legacy)) return ODatabaseState.Offline;
    else if ("not_available".equalsIgnoreCase(legacy)) return ODatabaseState.NotAvailable;
    return null;
  }

  private static ODatabaseState fromStableId(byte id) {
    return switch (id) {
      case 1 -> NotAvailable;
      case 2 -> Online;
      case 3 -> Offline;
      default -> null;
    };
  }

  public ODistributedServerManager.DB_STATUS toStatus() {
    switch (this) {
      case Online:
        {
          return ODistributedServerManager.DB_STATUS.ONLINE;
        }
      case Offline:
        {
          return ODistributedServerManager.DB_STATUS.OFFLINE;
        }
      case NotAvailable:
        {
          return ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
        }
    }
    return null;
  }
}
