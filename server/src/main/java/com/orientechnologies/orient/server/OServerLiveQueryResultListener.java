package com.orientechnologies.orient.server;

import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OLiveQueryInterruptedException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by tglman on 19/06/17.
 */
class OServerLiveQueryResultListener implements OLiveQueryResultListener {
  private final ONetworkProtocolBinary protocol;
  private       long                   monitorId;

  public OServerLiveQueryResultListener(ONetworkProtocolBinary protocol) {
    this.protocol = protocol;
  }

  public void setMonitorId(long monitorId) {
    this.monitorId = monitorId;
  }

  private void sendEvent(OLiveQueryResult event) {
    try {
      protocol.push(new OLiveQueryPushRequest(monitorId, OLiveQueryPushRequest.HAS_MORE, Collections.singletonList(event)));
    } catch (IOException e) {
      throw new OLiveQueryInterruptedException("Live query interrupted by socket close");
    }
  }

  @Override
  public void onCreate(ODatabaseDocument database, OResult data) {
    sendEvent(new OLiveQueryResult(OLiveQueryResult.CREATE_EVENT, data, null));
  }

  @Override
  public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
    sendEvent(new OLiveQueryResult(OLiveQueryResult.UPDATE_EVENT, after, before));
  }

  @Override
  public void onDelete(ODatabaseDocument database, OResult data) {
    sendEvent(new OLiveQueryResult(OLiveQueryResult.DELETE_EVENT, data, null));
  }

  @Override
  public void onError(ODatabaseDocument database) {
    try {
      protocol.push(new OLiveQueryPushRequest(monitorId, OLiveQueryPushRequest.END, Collections.emptyList()));
    } catch (IOException e) {
      throw new OLiveQueryInterruptedException("Live query interrupted by socket close");
    }
  }

  @Override
  public void onEnd(ODatabaseDocument database) {
    try {
      protocol.push(new OLiveQueryPushRequest(monitorId, OLiveQueryPushRequest.END, Collections.emptyList()));
    } catch (IOException e) {
      throw new OLiveQueryInterruptedException("Live query interrupted by socket close");
    }

  }
}
