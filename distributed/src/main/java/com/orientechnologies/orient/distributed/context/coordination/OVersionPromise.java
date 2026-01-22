package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.coordination.result.OOutdatedVersion;
import java.util.Optional;

public class OVersionPromise {

  private OVersion version;
  private Optional<OTransactionIdPromise> promise = Optional.empty();

  public OVersionPromise(OVersion version) {
    this.version = version;
  }

  public Optional<OAcceptResult> promise(OTransactionIdPromise promise, OVersion version) {
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
        return Optional.of(
            new OInvalidSequential(promised.getId().getSequence(), promise.getId().getSequence()));
      }
    }
  }

  public void accept(OTransactionIdPromise promise, OVersion version) {
    if (this.version.promise(version)) {
      this.version.accept(version);
      this.promise = Optional.empty();
    }
  }

  public OVersion next() {
    return this.version.next();
  }

  public OVersion getVersion() {
    return version;
  }
}
