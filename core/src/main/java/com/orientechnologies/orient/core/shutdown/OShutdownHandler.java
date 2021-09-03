package com.orientechnologies.orient.core.shutdown;

import com.orientechnologies.orient.core.Orient;

/**
 * Handler which is used inside of shutdown priority queue. The higher priority we have the earlier
 * this handler will be executed.
 *
 * <p>There are set of predefined priorities which are used for system shutdown handlers which
 * allows to add your handlers before , between and after them.
 *
 * @see Orient#addShutdownHandler(OShutdownHandler)
 * @see Orient#shutdown()
 */
public interface OShutdownHandler {
  /**
   * Priority of {@link com.orientechnologies.orient.core.Orient.OShutdownWorkersHandler} handler.
   */
  int SHUTDOWN_WORKERS_PRIORITY = 1000;

  /**
   * Priority of com.orientechnologies.orient.core.Orient.OShutdownPendingThreadsHandler handler.
   */
  int SHUTDOWN_PENDING_THREADS_PRIORITY = 1100;

  /** Priority of {@link Orient.OShutdownOrientDBInstancesHandler} handler. */
  int SHUTDOWN_ENGINES_PRIORITY = 1200;

  /** Priority of com.orientechnologies.orient.core.Orient.OShutdownProfilerHandler handler. */
  int SHUTDOWN_PROFILER_PRIORITY = 1300;

  /** Priority of com.orientechnologies.orient.core.Orient.OShutdownCallListenersHandler handler. */
  int SHUTDOWN_CALL_LISTENERS = 1400;

  /** @return Handlers priority. */
  int getPriority();

  /**
   * Code which executed during system shutdown. During call of {@link Orient#shutdown()} method
   * which is called during JVM shutdown.
   */
  void shutdown() throws Exception;
}
