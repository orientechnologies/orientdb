package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import java.util.Optional;
import java.util.UUID;

public class OTransactionId {
  private UUID           requester;
  private long           sequence;
  private Optional<Long> lastSuccessful;
  private Optional<UUID> lastRequester;

  public OTransactionId(UUID requester, long sequence, Optional<Long> lastSuccessful, Optional<UUID> lastRequester) {
    this.requester = requester;
    this.sequence = sequence;
    this.lastSuccessful = lastSuccessful;
    this.lastRequester = lastRequester;
  }

  public UUID getRequester() {
    return requester;
  }

  public void setRequester(UUID requester) {
    this.requester = requester;
  }

  public long getSequence() {
    return sequence;
  }

  public void setSequence(long sequence) {
    this.sequence = sequence;
  }

  public Optional<Long> getLastSuccessful() {
    return lastSuccessful;
  }

  public void setLastSuccessful(Optional<Long> lastSuccessful) {
    this.lastSuccessful = lastSuccessful;
  }

  public Optional<UUID> getLastRequester() {
    return lastRequester;
  }

  public void setLastRequester(Optional<UUID> lastRequester) {
    this.lastRequester = lastRequester;
  }
}
