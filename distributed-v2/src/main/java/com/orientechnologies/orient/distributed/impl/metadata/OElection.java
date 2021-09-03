package com.orientechnologies.orient.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class OElection {
  private final UUID electionId;
  private final int quorum;
  private final List<OElectionReply> replies = new ArrayList<>();
  private Optional<OElectionReply> elected;
  private final List<OElectionReply> logsStatus = new ArrayList<>();
  private Optional<OElectionReply> logChosen;

  public OElection(int quorum) {
    this.quorum = quorum;
    this.electionId = UUID.randomUUID();
  }

  public void addReply(ONodeIdentity sender, UUID electionId, Optional<OLogId> id) {
    if (this.electionId.equals(electionId)) {
      replies.add(new OElectionReply(sender, id));
    }
  }

  private static int compare(OElectionReply one, OElectionReply two) {
    if (one.getId().isPresent()) {
      if (two.getId().isPresent()) {
        return one.getId().get().compareTo(two.getId().get());
      } else {
        return 1;
      }
    } else if (two.getId().isPresent()) {
      return -1;
    } else {
      return 0;
    }
  }

  public synchronized Optional<OElectionReply> checkElection() {
    if (replies.size() > quorum && !elected.isPresent()) {
      replies.sort(OElection::compare);
      elected = Optional.of(replies.get(replies.size() - 1));
      return elected;
    }
    return Optional.empty();
  }

  public synchronized void addLastLog(ONodeIdentity sender, UUID electionId, Optional<OLogId> id) {
    if (this.electionId.equals(electionId)) {
      this.logsStatus.add(new OElectionReply(sender, id));
    }
  }

  public synchronized Optional<OElectionReply> checkLastLog() {
    if (logsStatus.size() > quorum && !logChosen.isPresent()) {
      logsStatus.sort(OElection::compare);
      OElectionReply leader = replies.get(replies.size() - 1);
      OElectionReply value = logsStatus.get(logsStatus.size() - quorum);
      logChosen = Optional.of(new OElectionReply(leader.getSender(), value.getId()));
      return logChosen;
    }
    return Optional.empty();
  }

  public UUID getElectionId() {
    return electionId;
  }
}
