package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import java.util.concurrent.atomic.AtomicBoolean;

public final class EventWrapper {
  private final Runnable event;
  private final AtomicBoolean fired = new AtomicBoolean(false);

  public EventWrapper(Runnable event) {
    this.event = event;
  }

  public void fire() {
    if (!fired.get() && fired.compareAndSet(false, true)) {
      event.run();
    }
  }
}
