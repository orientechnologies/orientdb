package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum ODatabaseState {
  NotAvailable,
  Synchronizing,
  Online,
  Offline,
  Backup;

  public static ODatabaseState readNetwork(DataInput input) throws IOException {
    short s = input.readShort();
    return ODatabaseState.values()[s];
  }

  public void writeNetwork(DataOutput out) throws IOException {
    // TODO: make sure this is network compatible
    out.writeShort(this.ordinal());
  }

  public static ODatabaseState from(ODistributedServerManager.DB_STATUS status) {
    switch (status) {
      case ONLINE:
        return ODatabaseState.Online;
      case OFFLINE:
        return ODatabaseState.Offline;

      case BACKUP:
        return ODatabaseState.Backup;
      case NOT_AVAILABLE:
        return ODatabaseState.NotAvailable;
      case SYNCHRONIZING:
        return ODatabaseState.Synchronizing;
    }
    return null;
  }

  public ODistributedServerManager.DB_STATUS toSatus() {
    switch (this) {
      case Online:
        {
          return ODistributedServerManager.DB_STATUS.ONLINE;
        }
      case Offline:
        {
          return ODistributedServerManager.DB_STATUS.OFFLINE;
        }
      case Backup:
        {
          return ODistributedServerManager.DB_STATUS.BACKUP;
        }
      case NotAvailable:
        {
          return ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
        }
      case Synchronizing:
        {
          return ODistributedServerManager.DB_STATUS.SYNCHRONIZING;
        }
    }
    return null;
  }
}
