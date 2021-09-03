package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.operationsfreezer;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableInteger;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

public final class OperationsFreezer {
  private final LongAdder operationsCount = new LongAdder();
  private final AtomicInteger freezeRequests = new AtomicInteger();

  private final WaitingList operationsWaitingList = new WaitingList();

  private final AtomicLong freezeIdGen = new AtomicLong();
  private final ConcurrentMap<Long, FreezeParameters> freezeParametersIdMap =
      new ConcurrentHashMap<>();

  private final ThreadLocal<OModifiableInteger> operationDepth =
      ThreadLocal.withInitial(OModifiableInteger::new);

  public void startOperation() {
    final OModifiableInteger operationDepth = this.operationDepth.get();
    if (operationDepth.value == 0) {
      operationsCount.increment();

      while (freezeRequests.get() > 0) {
        assert freezeRequests.get() >= 0;

        operationsCount.decrement();

        throwFreezeExceptionIfNeeded();

        final Thread thread = Thread.currentThread();

        operationsWaitingList.addThreadInWaitingList(thread);

        if (freezeRequests.get() > 0) {
          LockSupport.park(this);
        }

        operationsCount.increment();
      }
    }

    assert freezeRequests.get() >= 0;

    operationDepth.increment();
  }

  public void endOperation() {
    final OModifiableInteger operationDepth = this.operationDepth.get();
    if (operationDepth.value <= 0) {
      throw new IllegalStateException("Invalid operation depth " + operationDepth.value);
    } else {
      operationDepth.value--;
    }

    if (operationDepth.value == 0) {
      operationsCount.decrement();
    }
  }

  public long freezeOperations(
      final Class<? extends OException> exceptionClass, final String message) {
    final long id = freezeIdGen.incrementAndGet();

    freezeRequests.incrementAndGet();

    if (exceptionClass != null) {
      freezeParametersIdMap.put(id, new FreezeParameters(message, exceptionClass));
    }

    while (operationsCount.sum() > 0) {
      Thread.yield();
    }

    return id;
  }

  public void releaseOperations(final long id) {
    if (id >= 0) {
      freezeParametersIdMap.remove(id);
    }

    final Map<Long, FreezeParameters> freezeParametersMap = new HashMap<>(freezeParametersIdMap);
    final long requests = freezeRequests.decrementAndGet();

    if (requests == 0) {
      for (Long freezeId : freezeParametersMap.keySet()) {
        freezeParametersIdMap.remove(freezeId);
      }

      WaitingListNode node = operationsWaitingList.cutWaitingList();

      while (node != null) {
        LockSupport.unpark(node.item);
        node = node.next;
      }
    }
  }

  private void throwFreezeExceptionIfNeeded() {
    for (FreezeParameters freezeParameters : freezeParametersIdMap.values()) {
      assert freezeParameters.exceptionClass != null;

      if (freezeParameters.message != null) {
        try {
          final Constructor<? extends OException> mConstructor =
              freezeParameters.exceptionClass.getConstructor(String.class);
          throw mConstructor.newInstance(freezeParameters.message);
        } catch (InstantiationException
            | IllegalAccessException
            | NoSuchMethodException
            | SecurityException
            | InvocationTargetException ie) {
          OLogManager.instance()
              .error(
                  this,
                  "Can not create instance of exception "
                      + freezeParameters.exceptionClass
                      + " with message will try empty constructor instead",
                  ie);
          throwFreezeExceptionWithoutMessage(freezeParameters);
        }
      } else {
        throwFreezeExceptionWithoutMessage(freezeParameters);
      }
    }
  }

  private void throwFreezeExceptionWithoutMessage(FreezeParameters freezeParameters) {
    try {
      //noinspection deprecation
      throw freezeParameters.exceptionClass.newInstance();
    } catch (InstantiationException | IllegalAccessException ie) {
      OLogManager.instance()
          .error(
              this,
              "Can not create instance of exception "
                  + freezeParameters.exceptionClass
                  + " will park thread instead of throwing of exception",
              ie);
    }
  }

  private static final class FreezeParameters {
    private final String message;
    private final Class<? extends OException> exceptionClass;

    private FreezeParameters(String message, Class<? extends OException> exceptionClass) {
      this.message = message;
      this.exceptionClass = exceptionClass;
    }
  }
}
