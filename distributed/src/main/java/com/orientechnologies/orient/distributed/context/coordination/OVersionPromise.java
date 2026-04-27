package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OOutdatedVersion;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.util.Optional;

public class OVersionPromise {
  private static final OLoggerDistributed logger = OLoggerDistributed.logger(OVersionPromise.class);
  private final ONodeId current;
  private OVersion version;
  private Optional<OTransactionIdPromise> promise = Optional.empty();

  public OVersionPromise(OVersion version, ONodeId current) {
    this.version = version;
    this.current = current;
  }

  public synchronized Optional<OAcceptResult> promise(
      OTransactionIdPromise promise, OVersion version) {
    if (this.promise.isEmpty()) {
      if (this.version.promise(version)) {
        logger.debugNode(current, "version promising %s", promise);
        this.promise = Optional.of(promise);
        return Optional.empty();
      } else {
        return Optional.of(new OOutdatedVersion(version.getValue(), this.version.getValue()));
      }
    } else {
      var promised = this.promise.get();
      if (promised.nextAccept(promise)) {
        if (this.version.promise(version)) {
          logger.debugNode(current, "version promising %s", promise);
          this.promise = Optional.of(promise);
          return Optional.empty();
        } else {
          logger.debugNode(
              current, "outdated version %s~%s on promising %s", this.version, version, promise);
          return Optional.of(new OOutdatedVersion(version.getValue(), this.version.getValue()));
        }
      } else {
        logger.debugNode(current, "already promised %s on promising %s", this.promise, promise);
        return Optional.of(new OAlreadyPromised(this.promise.get().getCoordinator()));
      }
    }
  }

  public synchronized void accept(OTransactionIdPromise promise, OVersion version) {
    if (this.version.promise(version) && this.promise.map((x) -> x.equals(promise)).orElse(false)) {
      this.version = version;
      this.promise = Optional.empty();
    }
  }

  public synchronized void cancel(OTransactionIdPromise promise) {
    if (this.promise.isPresent() && this.promise.get().equals(promise)) {
      logger.debugNode(current, "canceling version promise %s", this.promise.get());
      this.promise = Optional.empty();
    }
  }

  public synchronized OVersion next() {
    return this.version.next();
  }

  public synchronized OVersion getVersion() {
    return version;
  }

  public synchronized void loadVersion(OVersion version) {
    this.version = version;
    assert promise.isEmpty();
  }

  public synchronized void forceVersion(OVersion version) {
    this.promise = Optional.empty();
    this.version = version;
  }
}
