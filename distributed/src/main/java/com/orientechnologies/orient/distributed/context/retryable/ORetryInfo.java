package com.orientechnologies.orient.distributed.context.retryable;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.Random;

public class ORetryInfo {

  private int retryCountDown;
  private int delay;
  private Random random = new Random();

  public ORetryInfo(int retryCountDown, int delay) {
    this.retryCountDown = retryCountDown;
    this.delay = delay;
  }

  public Optional<Integer> nextRetry() {
    if (this.retryCountDown > 0) {
      this.retryCountDown--;
      int delay = random.nextInt(this.delay);
      // Next retry will have longer dalay
      this.delay = this.delay + delay;
      return Optional.of(delay);
    } else {
      return Optional.empty();
    }
  }

  public boolean isFinished() {
    return this.retryCountDown == 0;
  }

  @Override
  public String toString() {
    return MessageFormat.format(
        "current delay {0} remaining retry {1}", this.delay, this.retryCountDown);
  }
}
