package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.log.OLogger.Level;
import com.orientechnologies.common.log.OLoggerFromManager;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

public final class OPoliglotLogger extends Handler {
  private OLogManager instance;

  public OPoliglotLogger(OLogManager instance) {
    this.instance = instance;
  }

  @Override
  public void publish(LogRecord record) {
    Level level = OLoggerFromManager.translateJavaLogging(record.getLevel());
    OLogger logger = instance.logger(OPoliglotLogger.class); // TODO:use passed class name
    logger.log(level, record.getMessage(), record.getThrown(), false, record.getParameters());
  }

  @Override
  public void flush() {
    instance.flush();
  }

  @Override
  public void close() throws SecurityException {
    instance.flush();
  }
}
