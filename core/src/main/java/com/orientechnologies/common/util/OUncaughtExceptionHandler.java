package com.orientechnologies.common.util;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;

import java.util.Collection;

/**
 * Handler which is used to log all exceptions which are left uncaught by any exception handler.
 */
public class OUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
  @Override
  public void uncaughtException(Thread thread, Throwable e) {
    final OLogManager logManager = OLogManager.instance();

    if (logManager != null) {
      OLogManager.instance().errorNoDb(this, "Uncaught exception in thread %s", e, thread.getName());

      if (e instanceof OutOfMemoryError) {
        final Collection<String> queries = OSQLDumper.dumpAllSQLQueries();
        if (queries.isEmpty())
          OLogManager.instance().errorNoDb(this, "Uncaught exception in thread %s", e, thread.getName());
        else {
          final StringBuilder sb = new StringBuilder();
          sb.append("OOM Error was thrown by JVM. OOM can be caused by one of the following queries: \n");
          sb.append("-----------------------------------------------------------------------------------\n");
          for (String query : queries) {
            sb.append("- '").append(query).append("'\n");
          }
          sb.append("-----------------------------------------------------------------------------------\n");

          OLogManager.instance().errorNoDb(this, sb.toString(), e);
        }
      } else {
        OLogManager.instance().errorNoDb(this, "Uncaught exception in thread %s", e, thread.getName());
      }

    }
  }
}
