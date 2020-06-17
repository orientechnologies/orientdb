/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.log;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Filter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Centralized Log Manager.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OLogManager {
  private static final String DEFAULT_LOG = "com.orientechnologies";
  private static final String ENV_INSTALL_CUSTOM_FORMATTER = "orientdb.installCustomFormatter";
  private static final OLogManager instance = new OLogManager();
  private boolean debug = false;
  private boolean info = true;
  private boolean warn = true;
  private boolean error = true;
  private Level minimumLevel = Level.SEVERE;

  private final AtomicBoolean shutdownFlag = new AtomicBoolean();
  private final ConcurrentMap<String, Logger> loggersCache = new ConcurrentHashMap<>();

  private static final AtomicReference<StorageFilter> storageFilterHolder = new AtomicReference<>();

  /**
   * Labels are needed to filer different kind of messages, for example to filter exceptions which
   * should be logged during the tests
   */
  private enum LogLabel {
    STORAGE_LOGGING
  }

  protected OLogManager() {}

  public static OLogManager instance() {
    return instance;
  }

  public void installCustomFormatter() {
    final boolean installCustomFormatter =
        Boolean.parseBoolean(
            OSystemVariableResolver.resolveSystemVariables(
                "${" + ENV_INSTALL_CUSTOM_FORMATTER + "}", "true"));

    if (!installCustomFormatter) return;

    try {
      // ASSURE TO HAVE THE ORIENT LOG FORMATTER TO THE CONSOLE EVEN IF NO CONFIGURATION FILE IS
      // TAKEN
      final Logger log = Logger.getLogger("");

      setLevelInternal(log.getLevel());

      if (log.getHandlers().length == 0) {
        // SET DEFAULT LOG FORMATTER
        final Handler h = new ConsoleHandler();
        h.setFormatter(new OAnsiLogFormatter());
        log.addHandler(h);
      } else {
        for (Handler h : log.getHandlers()) {
          if (h instanceof ConsoleHandler
              && !h.getFormatter().getClass().equals(OAnsiLogFormatter.class))
            h.setFormatter(new OAnsiLogFormatter());
        }
      }
    } catch (Exception e) {
      System.err.println(
          "Error while installing custom formatter. Logging could be disabled. Cause: "
              + e.toString());
    }
  }

  public void setConsoleLevel(final String iLevel) {
    setLevel(iLevel, ConsoleHandler.class);
  }

  public void setFileLevel(final String iLevel) {
    setLevel(iLevel, FileHandler.class);
  }

  /**
   * Start filter out all messages which are logged using {@link #errorStorage(Object, String,
   * Throwable, Object...)} method.
   */
  public void applyStorageFilter() {
    final StorageFilter filter = new StorageFilter();

    if (storageFilterHolder.compareAndSet(null, filter)) {
      for (Logger logger : loggersCache.values()) {
        logger.setFilter(filter);
      }
    }
  }

  public void log(
      final Object iRequester,
      final Level iLevel,
      String iMessage,
      final Throwable iException,
      final boolean extractDatabase,
      final LogLabel label,
      final Object... iAdditionalArgs) {

    if (shutdownFlag.get()) {
      System.err.println("ERROR: LogManager is shutdown, no logging is possible !!!");
      return;
    }

    if (iMessage != null) {
      if (extractDatabase)
        try {
          final ODatabaseDocumentInternal db =
              ODatabaseRecordThreadLocal.instance() != null
                  ? ODatabaseRecordThreadLocal.instance().getIfDefined()
                  : null;
          if (db != null
              && db.getStorage() != null
              && db.getStorage() instanceof OAbstractPaginatedStorage) {
            final String dbName = db.getStorage().getName();
            if (dbName != null) iMessage = "$ANSI{green {db=" + dbName + "}} " + iMessage;
          }
        } catch (Exception ignore) {
        }

      final String requesterName;
      if (iRequester instanceof Class<?>) {
        requesterName = ((Class<?>) iRequester).getName();
      } else if (iRequester != null) {
        requesterName = iRequester.getClass().getName();
      } else {
        requesterName = DEFAULT_LOG;
      }

      Logger log = loggersCache.get(requesterName);
      if (log == null) {
        log = Logger.getLogger(requesterName);

        if (log != null) {
          Logger oldLogger = loggersCache.putIfAbsent(requesterName, log);

          if (oldLogger != null) {
            log = oldLogger;
          } else {
            StorageFilter filter = storageFilterHolder.get();
            log.setFilter(filter);
          }
        }
      }

      if (log == null) {
        // USE SYSERR
        try {
          System.err.println(String.format(iMessage, iAdditionalArgs));
        } catch (Exception e) {
          System.err.print(
              String.format(
                  "Error on formatting message '%s'. Exception: %s", iMessage, e.toString()));
        }
      } else if (log.isLoggable(iLevel)) {
        // USE THE LOG
        try {
          final String msg = String.format(iMessage, iAdditionalArgs);
          final LogRecord record = new LogRecord(iLevel, msg);
          record.setLoggerName(log.getName());
          if (iException != null) {
            record.setThrown(iException);
          }

          if (label != null) {
            record.setParameters(new Object[] {label});
          }

          log.log(record);
        } catch (Exception e) {
          System.err.print(
              String.format(
                  "Error on formatting message '%s'. Exception: %s", iMessage, e.toString()));
        }
      }
    }
  }

  public void debug(
      final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    if (isDebugEnabled()) log(iRequester, Level.FINE, iMessage, null, true, null, iAdditionalArgs);
  }

  public void debug(
      final Object iRequester,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    if (isDebugEnabled())
      log(iRequester, Level.FINE, iMessage, iException, true, null, iAdditionalArgs);
  }

  public void debugNoDb(
      final Object iRequester,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    if (isDebugEnabled())
      log(iRequester, Level.FINE, iMessage, iException, false, null, iAdditionalArgs);
  }

  public void info(
      final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    if (isInfoEnabled()) log(iRequester, Level.INFO, iMessage, null, true, null, iAdditionalArgs);
  }

  public void infoNoDb(
      final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    if (isInfoEnabled()) log(iRequester, Level.INFO, iMessage, null, false, null, iAdditionalArgs);
  }

  public void info(
      final Object iRequester,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    if (isInfoEnabled())
      log(iRequester, Level.INFO, iMessage, iException, true, null, iAdditionalArgs);
  }

  public void warn(
      final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    if (isWarnEnabled())
      log(iRequester, Level.WARNING, iMessage, null, true, null, iAdditionalArgs);
  }

  public void warnNoDb(
      final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    if (isWarnEnabled())
      log(iRequester, Level.WARNING, iMessage, null, false, null, iAdditionalArgs);
  }

  public void warn(
      final Object iRequester,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    if (isWarnEnabled())
      log(iRequester, Level.WARNING, iMessage, iException, true, null, iAdditionalArgs);
  }

  public void config(
      final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    log(iRequester, Level.CONFIG, iMessage, null, true, null, iAdditionalArgs);
  }

  public void error(
      final Object iRequester,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    if (isErrorEnabled())
      log(iRequester, Level.SEVERE, iMessage, iException, true, null, iAdditionalArgs);
  }

  public void errorNoDb(
      final Object iRequester,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    if (isErrorEnabled())
      log(iRequester, Level.SEVERE, iMessage, iException, false, null, iAdditionalArgs);
  }

  public void errorStorage(
      final Object iRequester,
      final String iMessage,
      final Throwable iException,
      final Object... iAdditionalArgs) {
    if (isErrorEnabled())
      log(
          iRequester,
          Level.SEVERE,
          iMessage,
          iException,
          false,
          LogLabel.STORAGE_LOGGING,
          iAdditionalArgs);
  }

  public boolean isWarn() {
    return warn;
  }

  public boolean isLevelEnabled(final Level level) {
    if (level.equals(Level.FINER) || level.equals(Level.FINE) || level.equals(Level.FINEST))
      return debug;
    else if (level.equals(Level.INFO)) return info;
    else if (level.equals(Level.WARNING)) return warn;
    else if (level.equals(Level.SEVERE)) return error;
    return false;
  }

  public boolean isDebugEnabled() {
    return debug;
  }

  public void setDebugEnabled(boolean debug) {
    this.debug = debug;
  }

  public boolean isInfoEnabled() {
    return info;
  }

  public void setInfoEnabled(boolean info) {
    this.info = info;
  }

  public boolean isWarnEnabled() {
    return warn;
  }

  public void setWarnEnabled(boolean warn) {
    this.warn = warn;
  }

  public boolean isErrorEnabled() {
    return error;
  }

  public void setErrorEnabled(boolean error) {
    this.error = error;
  }

  public Level setLevel(final String iLevel, final Class<? extends Handler> iHandler) {
    final Level level =
        iLevel != null ? Level.parse(iLevel.toUpperCase(Locale.ENGLISH)) : Level.INFO;

    if (level.intValue() < minimumLevel.intValue()) {
      // UPDATE MINIMUM LEVEL
      minimumLevel = level;

      setLevelInternal(level);
    }

    Logger log = Logger.getLogger(DEFAULT_LOG);
    while (log != null) {

      for (Handler h : log.getHandlers()) {
        if (h.getClass().isAssignableFrom(iHandler)) {
          h.setLevel(level);
          break;
        }
      }

      log = log.getParent();
    }

    return level;
  }

  protected void setLevelInternal(final Level level) {
    if (level == null) return;

    if (level.equals(Level.FINER) || level.equals(Level.FINE) || level.equals(Level.FINEST))
      debug = info = warn = error = true;
    else if (level.equals(Level.INFO)) {
      info = warn = error = true;
      debug = false;
    } else if (level.equals(Level.WARNING)) {
      warn = error = true;
      debug = info = false;
    } else if (level.equals(Level.SEVERE)) {
      error = true;
      debug = info = warn = false;
    }
  }

  public void flush() {
    for (Handler h : Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).getHandlers()) h.flush();
  }

  public OCommandOutputListener getCommandOutputListener(final Object iThis, final Level iLevel) {
    return new OCommandOutputListener() {
      @Override
      public void onMessage(String iText) {
        log(iThis, iLevel, iText, null, true, null);
      }
    };
  }

  /** Shutdowns this log manager. */
  public void shutdown() {
    if (shutdownFlag.compareAndSet(false, true)) {
      try {
        if (LogManager.getLogManager() instanceof ShutdownLogManager)
          ((ShutdownLogManager) LogManager.getLogManager()).shutdown();
      } catch (NoClassDefFoundError ignore) {
        // Om nom nom. Some custom class loaders, like Tomcat's one, cannot load classes while in
        // shutdown hooks, since their
        // runtime is already shutdown. Ignoring the exception, if ShutdownLogManager is not loaded
        // at this point there are no instances
        // of it anyway and we have nothing to shutdown.
      }
    }
  }

  /**
   * @return <code>true</code> if log manager is shutdown by {@link #shutdown()} method and no
   *     logging is possible.
   */
  public boolean isShutdown() {
    return shutdownFlag.get();
  }

  /**
   * Removes all message which were issued by {@link OLogManager#errorStorage(Object, String,
   * Throwable, Object...)} method.
   */
  private static class StorageFilter implements Filter {
    @Override
    public boolean isLoggable(LogRecord record) {
      Object[] params = record.getParameters();
      if (params != null) {
        for (Object param : params) {
          if (LogLabel.STORAGE_LOGGING == param) {
            return false;
          }
        }
      }

      return true;
    }
  }
}
