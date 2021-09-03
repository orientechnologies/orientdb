package com.orientechnologies.orient.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

public class OElectionContext {

  private Timer timer;
  private Map<String, OElection> elections = new HashMap<>();

  public OElectionContext() {
    timer = new Timer();
  }

  public synchronized UUID startElection(String name, int quorum) {
    OElection election = new OElection(quorum);
    elections.put(name, election);
    // TODO: configure timeut
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            elections.remove(name);
          }
        },
        60000);
    return election.getElectionId();
  }

  public synchronized Optional<OLogId> received(
      ONodeIdentity sender, String database, UUID electionId, Optional<OLogId> id) {
    OElection election = elections.get(database);
    if (election != null) {
      election.addReply(sender, electionId, id);
      Optional<OElectionReply> elected = election.checkElection();
      if (elected.isPresent()) {
        OLogId electedId = elected.get().getId().get();
        return Optional.of(electedId);
      } else {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  public synchronized Optional<OElectionReply> receivedLastLog(
      ONodeIdentity sender, UUID electionId, String database, Optional<OLogId> id) {
    OElection election = elections.get(database);
    if (election != null) {
      election.addLastLog(sender, electionId, id);
      return election.checkLastLog();
    }
    return Optional.empty();
  }
}
