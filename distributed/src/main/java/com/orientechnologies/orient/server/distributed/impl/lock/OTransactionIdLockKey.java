package com.orientechnologies.orient.server.distributed.impl.lock;

import com.orientechnologies.orient.core.tx.OTransactionId;
import java.util.Objects;

public class OTransactionIdLockKey implements OLockKey {
  private OTransactionId transactionId;

  public OTransactionIdLockKey(OTransactionId transactionId) {
    if (transactionId == null) {
      throw new NullPointerException();
    }
    this.transactionId = transactionId;
  }

  @Override
  public boolean equals(Object obj) {
    // It use just the position to make sure that the operation for a position are sequential
    if (obj instanceof OTransactionIdLockKey) {
      return this.transactionId.getPosition()
          == ((OTransactionIdLockKey) obj).transactionId.getPosition();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    // It use just the position to make sure that the operation for a position are sequential
    return Objects.hash(this.transactionId.getPosition());
  }
}
