/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.cloudbees.syslog.integration.jul;

import com.cloudbees.syslog.integration.jul.util.LogManagerHelper;
import java.util.logging.*;

/** @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a> */
public abstract class AbstractHandler extends Handler {

  private Level logLevel = Level.ALL;
  private Filter filter;
  private Formatter formatter;

  public AbstractHandler() {
    super();
    LogManager manager = LogManager.getLogManager();
    String cname = getClass().getName();
    this.logLevel = LogManagerHelper.getLevelProperty(manager, cname + ".level", Level.INFO);
    this.filter = LogManagerHelper.getFilterProperty(manager, cname + ".filter", null);
    this.formatter =
        LogManagerHelper.getFormatterProperty(manager, cname + ".formatter", getDefaultFormatter());
  }

  public AbstractHandler(Level level, Filter filter) {
    this.logLevel = level;
    this.filter = filter;
    this.formatter = getDefaultFormatter();
  }

  /**
   * For extensibility
   *
   * @return
   */
  protected Formatter getDefaultFormatter() {
    return new SimpleFormatter();
  }

  /** {@inheritDoc} */
  public boolean isLoggable(LogRecord record) {
    if (record == null) {
      return false;
    }
    return super.isLoggable(record);
  }

  /** {@inheritDoc} */
  @Override
  public Level getLevel() {
    return this.logLevel;
  }

  /** {@inheritDoc} */
  public Filter getFilter() {
    return filter;
  }

  /** {@inheritDoc} */
  @Override
  public Formatter getFormatter() {
    return formatter;
  }

  /** {@inheritDoc} */
  @Override
  public void setFormatter(Formatter formatter) throws SecurityException {
    this.formatter = formatter;
  }
}
