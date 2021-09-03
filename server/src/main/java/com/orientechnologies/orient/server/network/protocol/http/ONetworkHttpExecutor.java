package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;

public interface ONetworkHttpExecutor {

  String getRemoteAddress();

  void setDatabase(ODatabaseDocumentInternal db);
}
