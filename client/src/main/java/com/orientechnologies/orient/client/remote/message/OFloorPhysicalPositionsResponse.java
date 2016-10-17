package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

public class OFloorPhysicalPositionsResponse implements OBinaryResponse<OPhysicalPosition[]> {
  @Override
  public OPhysicalPosition[] read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    final int positionsCount = network.readInt();

    if (positionsCount == 0) {
      return OCommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
    } else {
      return OStorageRemote.readPhysicalPositions(network, positionsCount);
    }
  }
}