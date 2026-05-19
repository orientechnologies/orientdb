package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.Random;

public class ORetryInfo {
  private static final OLogger logger = OLogManager.instance().logger(ORetryInfo.class);
  private final int maxRetry;
  private int retryCount = 0;
  private final int delay;
  private final Random random = new Random();

  public ORetryInfo(int maxRetry, int delay) {
    if (maxRetry <= 0) {
      logger.warn("Retry configured with an invalid value:%s assuming 1", maxRetry);
      maxRetry = 1;
    }
    // Add a random extra retry to increase the likeness of one reaching the success.
    int extra = 0;
    if (maxRetry / 2 > 0) {
      extra = random.nextInt(maxRetry / 2);
    }
    this.maxRetry = maxRetry + extra;
    this.delay = delay;
  }

  public Optional<Integer> nextRetry() {
    if (isFinished()) {
      return Optional.empty();
    } else {
      this.retryCount++;
      int delay = random.nextInt(this.delay * retryCount);
      return Optional.of(delay);
    }
  }

  public boolean isFinished() {
    return this.retryCount >= this.maxRetry;
  }

  @Override
  public String toString() {
    return MessageFormat.format(
        "current delay {0} remaining retry {1}", this.delay, this.maxRetry - this.retryCount);
  }
}
