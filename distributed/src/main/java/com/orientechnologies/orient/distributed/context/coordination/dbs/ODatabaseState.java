package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum ODatabaseState {
  NotAvailable,
  Online,
  Offline;

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
      case NOT_AVAILABLE:
        return ODatabaseState.NotAvailable;
    }
    return null;
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
