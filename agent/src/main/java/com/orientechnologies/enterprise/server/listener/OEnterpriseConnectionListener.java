package com.orientechnologies.enterprise.server.listener;

import com.orientechnologies.orient.server.OClientConnection;

/**
 * Created by Enrico Risa on 16/07/2018.
 */
public interface OEnterpriseConnectionListener {

  default void onClientConnection(final OClientConnection iConnection) {

  }

  default void onClientDisconnection(final OClientConnection iConnection) {
  }

  default void onBeforeClientRequest(final OClientConnection iConnection, final byte iRequestType) {
  }

  default void onAfterClientRequest(final OClientConnection iConnection, final byte iRequestType) {
  }

  default void onClientError(final OClientConnection iConnection, final Throwable iThrowable) {
  }
}
