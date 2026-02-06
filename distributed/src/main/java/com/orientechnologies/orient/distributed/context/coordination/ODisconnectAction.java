package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.List;

public sealed interface ODisconnectAction
    permits ODisconnectAction.OReconsentPromised, ODisconnectAction.ONothingToDo {

  void execute(OrientDBDistributed context);

  public record OReconsentPromised(List<ODistributedMessage> promised)
      implements ODisconnectAction {

    @Override
    public void execute(OrientDBDistributed context) {
      // TODO: remove execution
      for (ODistributedMessage message : promised) {
        message.recoordinate(context);
      }
    }
  }

  public record ONothingToDo() implements ODisconnectAction {

    @Override
    public void execute(OrientDBDistributed context) {}
  }
}
