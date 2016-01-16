/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.common.log;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OLogManager {
  private static final String      DEFAULT_LOG                  = "com.orientechnologies";
  private static final String      ENV_INSTALL_CUSTOM_FORMATTER = "orientdb.installCustomFormatter";
  private static final OLogManager instance                     = new OLogManager();
  private boolean                  debug                        = true;
  private boolean                  info                         = true;
  private boolean                  warn                         = true;
  private boolean                  error                        = true;
  private Level                    minimumLevel                 = Level.SEVERE;

  private final ConcurrentMap<String, Logger> loggersCache = new ConcurrentHashMap<String, Logger>();

  protected OLogManager() {
  }

  public static OLogManager instance() {
    return instance;
  }

  public static void installCustomFormatter() {
    final boolean installCustomFormatter = Boolean
        .parseBoolean(OSystemVariableResolver.resolveSystemVariables("${" + ENV_INSTALL_CUSTOM_FORMATTER + "}", "true"));

    if (!installCustomFormatter)
      return;

    try {
      // ASSURE TO HAVE THE ORIENT LOG FORMATTER TO THE CONSOLE EVEN IF NO CONFIGURATION FILE IS TAKEN
      final Logger log = Logger.getLogger("");
      if (log.getHandlers().length == 0) {
        // SET DEFAULT LOG FORMATTER
        final Handler h = new ConsoleHandler();
        h.setFormatter(new OLogFormatter());
        log.addHandler(h);
      } else {
        for (Handler h : log.getHandlers()) {
          if (h instanceof ConsoleHandler && !h.getFormatter().getClass().equals(OLogFormatter.class))
            h.setFormatter(new OLogFormatter());
        }
      }
    } catch (Exception e) {
      System.err.println("Error while installing custom formatter. Logging could be disabled. Cause: " + e.toString());
    }
  }

  public void setConsoleLevel(final String iLevel) {
    setLevel(iLevel, ConsoleHandler.class);
  }

  public void setFileLevel(final String iLevel) {
    setLevel(iLevel, FileHandler.class);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException,
      final Object... iAdditionalArgs) {
    if (iMessage != null) {
      try {
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE != null
            ? ODatabaseRecordThreadLocal.INSTANCE.getIfDefined() : null;
        if (db != null && db.getStorage() != null && db.getStorage() instanceof OAbstractPaginatedStorage) {
          final String dbName = db.getStorage().getName();
          if (dbName != null)
            iMessage = "{db=" + dbName + "} " + iMessage;
        }
      } catch (Throwable e) {
      }

      final String requesterName;
      if (iRequester != null) {
        requesterName = iRequester.getClass().getName();
      } else {
        requesterName = DEFAULT_LOG;
      }

      Logger log = loggersCache.get(requesterName);
      if (log == null) {
        log = Logger.getLogger(requesterName);

        if (log != null) {
          Logger oldLogger = loggersCache.putIfAbsent(requesterName, log);

          if (oldLogger != null)
            log = oldLogger;
        }
      }

      if (log == null) {
        // USE SYSERR
        try {
          System.err.println(String.format(iMessage, iAdditionalArgs));
        } catch (Exception e) {
          OLogManager.instance().warn(this, "Error on formatting message", e);
        }
      } else if (log.isLoggable(iLevel)) {
        // USE THE LOG
        try {
          final String msg = String.format(iMessage, iAdditionalArgs);
          if (iException != null)
            log.log(iLevel, msg, iException);
          else
            log.log(iLevel, msg);
        } catch (Exception e) {
          System.err.print(String.format("Error on formatting message '%s'. Exception: %s", iMessage, e.toString()));
        }
      }
    }
  }

  public void debug(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    if (isDebugEnabled())
      log(iRequester, Level.FINE, iMessage, null, iAdditionalArgs);
  }

  public void debug(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
    if (isDebugEnabled())
      log(iRequester, Level.FINE, iMessage, iException, iAdditionalArgs);
  }

  public void debug(final Object iRequester, final String iMessage, final Throwable iException,
      final Class<? extends OException> iExceptionClass, final Object... iAdditionalArgs) {
    debug(iRequester, iMessage, iException, iAdditionalArgs);

    if (iExceptionClass != null)
      try {
        throw iExceptionClass.getConstructor(String.class, Throwable.class).newInstance(iMessage, iException);
      } catch (NoSuchMethodException e) {
      } catch (IllegalArgumentException e) {
      } catch (SecurityException e) {
      } catch (InstantiationException e) {
      } catch (IllegalAccessException e) {
      } catch (InvocationTargetException e) {
      }
  }

  public void info(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    if (isInfoEnabled())
      log(iRequester, Level.INFO, iMessage, null, iAdditionalArgs);
  }

  public void info(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
    if (isInfoEnabled())
      log(iRequester, Level.INFO, iMessage, iException, iAdditionalArgs);
  }

  public void warn(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    if (isWarnEnabled())
      log(iRequester, Level.WARNING, iMessage, null, iAdditionalArgs);
  }

  public void warn(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
    if (isWarnEnabled())
      log(iRequester, Level.WARNING, iMessage, iException, iAdditionalArgs);
  }

  public void config(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    log(iRequester, Level.CONFIG, iMessage, null, iAdditionalArgs);
  }

  public void error(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    log(iRequester, Level.SEVERE, iMessage, null, iAdditionalArgs);
  }

  public void error(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
    if (isErrorEnabled())
      log(iRequester, Level.SEVERE, iMessage, iException, iAdditionalArgs);
  }

  public void error(final Object iRequester, final String iMessage, final Throwable iException,
      final Class<? extends OException> iExceptionClass, final Object... iAdditionalArgs) {
    error(iRequester, iMessage, iException, iAdditionalArgs);

    final String msg = String.format(iMessage, iAdditionalArgs);

    if (iExceptionClass != null)
      try {
        throw iExceptionClass.getConstructor(String.class, Throwable.class).newInstance(msg, iException);
      } catch (NoSuchMethodException e) {
      } catch (IllegalArgumentException e) {
      } catch (SecurityException e) {
      } catch (InstantiationException e) {
      } catch (IllegalAccessException e) {
      } catch (InvocationTargetException e) {
      }
  }

  public void error(final Object iRequester, final String iMessage, final Class<? extends OException> iExceptionClass) {
    error(iRequester, iMessage, (Throwable) null);

    try {
      throw iExceptionClass.getConstructor(String.class).newInstance(iMessage);
    } catch (IllegalArgumentException e) {
    } catch (SecurityException e) {
    } catch (InstantiationException e) {
    } catch (IllegalAccessException e) {
    } catch (InvocationTargetException e) {
    } catch (NoSuchMethodException e) {
    }
  }

  @SuppressWarnings("unchecked")
  public void exception(final String iMessage, final Exception iNestedException, final Class<? extends OException> iExceptionClass,
      final Object... iAdditionalArgs) throws OException {
    if (iMessage == null)
      return;

    // FORMAT THE MESSAGE
    String msg = String.format(iMessage, iAdditionalArgs);

    Constructor<OException> c;
    OException exceptionToThrow = null;
    try {
      if (iNestedException != null) {
        c = (Constructor<OException>) iExceptionClass.getConstructor(String.class, Throwable.class);
        exceptionToThrow = c.newInstance(msg, iNestedException);
      }
    } catch (Exception e) {
    }

    if (exceptionToThrow == null)
      try {
        c = (Constructor<OException>) iExceptionClass.getConstructor(String.class);
        exceptionToThrow = c.newInstance(msg);
      } catch (SecurityException e1) {
      } catch (NoSuchMethodException e1) {
      } catch (IllegalArgumentException e1) {
      } catch (InstantiationException e1) {
      } catch (IllegalAccessException e1) {
      } catch (InvocationTargetException e1) {
      }

    if (exceptionToThrow != null)
      throw exceptionToThrow;
    else
      throw new IllegalArgumentException("Cannot create the exception of type: " + iExceptionClass);
  }

  public boolean isWarn() {
    return warn;
  }

  public boolean isLevelEnabled(final Level level) {
    if (level.equals(Level.FINER) || level.equals(Level.FINE) || level.equals(Level.FINEST))
      return debug;
    else if (level.equals(Level.INFO))
      return info;
    else if (level.equals(Level.WARNING))
      return warn;
    else if (level.equals(Level.SEVERE))
      return error;
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
    final Level level = iLevel != null ? Level.parse(iLevel.toUpperCase(Locale.ENGLISH)) : Level.INFO;

    if (level.intValue() < minimumLevel.intValue()) {
      // UPDATE MINIMUM LEVEL
      minimumLevel = level;

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

  public void flush() {
    for (Handler h : Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).getHandlers())
      h.flush();
  }

  public OCommandOutputListener getCommandOutputListener(final Object iThis, final Level iLevel) {
    return new OCommandOutputListener() {
      @Override
      public void onMessage(String iText) {
        log(iThis, iLevel, iText, null);
      }
    };
  }
}
