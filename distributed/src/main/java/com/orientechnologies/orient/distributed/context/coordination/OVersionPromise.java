package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OOutdatedVersion;
import java.util.Optional;

public class OVersionPromise {

  private OVersion version;
  private Optional<OTransactionIdPromise> promise = Optional.empty();

  public OVersionPromise(OVersion version) {
    this.version = version;
  }

  public synchronized Optional<OAcceptResult> promise(
      OTransactionIdPromise promise, OVersion version) {
    if (this.promise.isEmpty()) {
      if (this.version.promise(version)) {
        this.promise = Optional.of(promise);
        return Optional.empty();
      } else {
        return Optional.of(new OOutdatedVersion(version.getValue(), this.version.getValue()));
      }
    } else {
      var promised = this.promise.get();
      if (promised.nextAccept(promise)) {
        if (this.version.promise(version)) {
          this.promise = Optional.of(promise);
          return Optional.empty();
        } else {
          return Optional.of(new OOutdatedVersion(version.getValue(), this.version.getValue()));
        }
      } else {
        return Optional.of(new OAlreadyPromised());
      }
    }
  }

  public synchronized void accept(OTransactionIdPromise promise, OVersion version) {
    if (this.version.promise(version)) {
      this.version.accept(version);
      this.promise = Optional.empty();
    }
  }

  public synchronized void cancel(OTransactionIdPromise promise) {
    if (this.promise.isPresent() && this.promise.get().equals(promise)) {
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
}
