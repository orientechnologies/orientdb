package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OTransactionId;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/** This represent the applied state, in a normal flow this would be part of the storage
 * and this may lag behind the log because it depends on the time of the executions of apply.
 *
 */
public class OAppliedState {

  private final OWatchPromise[] watches;
  private OAppliedTransaction applied;

  public interface OAppliedTransaction {
    boolean isApplied(OTransactionId tx);
  }

  public OAppliedState(int size, OAppliedTransaction applied) {
    this.watches = new OWatchPromise[size];
    this.applied = applied;
  }

  public synchronized Optional<CountDownLatch> watch(OTransactionId id) {
    if (this.applied.isApplied(id)) {
      // Already applied, nothing to wait
      return Optional.empty();
    }
    OWatchPromise watch = this.watches[id.getPosition()];
    if (watch == null) {
      watch = new OWatchPromise();
      this.watches[id.getPosition()] = watch;
    }
    return Optional.of(watch.watch(id.getSequence()));
  }

  public synchronized void applied(OTransactionId id) {
    if (this.applied.isApplied(id)) {
      // No Op, already applied
      return;
    }
    OWatchPromise watch = this.watches[id.getPosition()];
    if (watch != null) {
      watch.complete(id.getSequence());
    }
  }
}
