package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OIncrementalBackupRequest implements OBinaryRequest {
  private final String backupDirectory;

  public OIncrementalBackupRequest(String backupDirectory) {
    this.backupDirectory = backupDirectory;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeString(backupDirectory);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP;
  }
}