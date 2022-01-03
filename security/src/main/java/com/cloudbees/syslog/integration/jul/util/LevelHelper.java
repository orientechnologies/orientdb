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
package com.cloudbees.syslog.integration.jul.util;

import com.cloudbees.syslog.Severity;
import java.util.*;
import java.util.logging.Level;

/** @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a> */
public class LevelHelper {

  public static final List<Level> levels =
      Collections.unmodifiableList(
          Arrays.asList(
              Level.OFF,
              Level.SEVERE,
              Level.WARNING,
              Level.INFO,
              Level.CONFIG,
              Level.FINE,
              Level.FINER,
              Level.FINEST,
              Level.ALL));

  public static final Map<String, Level> levelsByName;
  public static final Map<Integer, Level> levelsByValue;
  private static final Map<Level, Severity> julLevelToSyslogSeverity;

  static {
    Map<String, Level> levelsByNameMap = new HashMap<String, Level>();
    Map<Integer, Level> levelsByValueMap = new HashMap<Integer, Level>();
    for (Level level : levels) {
      levelsByNameMap.put(level.getName(), level);
      levelsByValueMap.put(level.intValue(), level);
    }
    levelsByName = Collections.unmodifiableMap(levelsByNameMap);
    levelsByValue = Collections.unmodifiableMap(levelsByValueMap);

    julLevelToSyslogSeverity =
        Collections.unmodifiableMap(
            new HashMap<Level, Severity>() {
              {
                put(Level.CONFIG, Severity.INFORMATIONAL);
                put(Level.FINE, Severity.DEBUG);
                put(Level.FINER, Severity.DEBUG);
                put(Level.FINEST, Severity.DEBUG);
                put(Level.INFO, Severity.INFORMATIONAL);
                put(Level.SEVERE, Severity.ERROR);
                put(Level.WARNING, Severity.WARNING);
              }
            });
  }

  /**
   * Simplification: use delegate to {@link Level#parse(String)} even if the behavior is slightly
   * different for localized log levels.
   *
   * @param name {@code null} or empty returns {@code null}
   * @return
   */
  public static Level findLevel(String name) {
    if (name == null || name.isEmpty()) return null;
    return Level.parse(name);
  }

  public static Severity toSeverity(Level level) {
    return julLevelToSyslogSeverity.get(level);
  }

  /** Compare on {@link java.util.logging.Level#intValue()} */
  public static Comparator<Level> comparator() {
    return new Comparator<Level>() {
      @Override
      public int compare(Level l1, Level l2) {
        return Integer.compare(l1.intValue(), l2.intValue());
      }
    };
  }
}
