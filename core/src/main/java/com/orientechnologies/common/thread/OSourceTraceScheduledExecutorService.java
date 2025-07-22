package com.orientechnologies.common.thread;

import java.util.Objects;
import java.util.concurrent.*;

public class OSourceTraceScheduledExecutorService extends OSourceTraceExecutorService
    implements ScheduledExecutorService {

  private final ScheduledExecutorService scheduledService;

  public OSourceTraceScheduledExecutorService(ScheduledExecutorService service) {
    super(service);
    Objects.requireNonNull(service);
    this.scheduledService = service;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(command);
    return scheduledService.schedule(
        () -> {
          try {
            command.run();
          } catch (RuntimeException e) {
            throw OTracedExecutionException.trace(trace, e, command);
          }
        },
        delay,
        unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(callable);
    return scheduledService.schedule(
        () -> {
          try {
            return callable.call();
          } catch (RuntimeException e) {
            throw OTracedExecutionException.trace(trace, e, callable);
          }
        },
        delay,
        unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(command);
    return scheduledService.scheduleAtFixedRate(
        () -> {
          try {
            command.run();
          } catch (RuntimeException e) {
            throw OTracedExecutionException.trace(trace, e, command);
          }
        },
        initialDelay,
        period,
        unit);
  }

  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(command);
    return scheduledService.scheduleWithFixedDelay(
        () -> {
          try {
            command.run();
          } catch (RuntimeException e) {
            throw OTracedExecutionException.trace(trace, e, command);
          }
        },
        initialDelay,
        delay,
        unit);
  }
}
