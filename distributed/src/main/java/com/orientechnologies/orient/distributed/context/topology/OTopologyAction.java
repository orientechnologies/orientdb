package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.distributed.db.OOperationMessage;

public interface OTopologyAction {

  void send(OOperationMessage message);

  void enstablish(OEnstablishTopology oEnstablishTopology);
}
