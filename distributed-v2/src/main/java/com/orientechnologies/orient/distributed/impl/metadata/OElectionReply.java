package com.orientechnologies.orient.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.util.Optional;

public class OElectionReply {
  private final ONodeIdentity sender;
  private final Optional<OLogId> id;

  public OElectionReply(ONodeIdentity sender, Optional<OLogId> id) {
    this.sender = sender;
    this.id = id;
  }

  public Optional<OLogId> getId() {
    return id;
  }

  public ONodeIdentity getSender() {
    return sender;
  }
}
