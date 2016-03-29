/*
 * Copyright (c) 2010-2013 the original author or authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 */
package com.cloudbees.syslog.integration.jul.util;

import com.cloudbees.syslog.Severity;

import java.util.*;
import java.util.logging.Level;

/**
 * @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a>
 */
public class LevelHelper {

    public final static List<Level> levels = Collections.unmodifiableList(Arrays.asList(Level.OFF, Level.SEVERE, Level.WARNING, Level.INFO, Level.CONFIG,
            Level.FINE, Level.FINER, Level.FINEST, Level.ALL));

    public final static Map<String, Level> levelsByName;
    public final static Map<Integer, Level> levelsByValue;
    private final static Map<Level, Severity> julLevelToSyslogSeverity;

    static {
        Map<String, Level> levelsByNameMap = new HashMap<String, Level>();
        Map<Integer, Level> levelsByValueMap = new HashMap<Integer, Level>();
        for (Level level : levels) {
            levelsByNameMap.put(level.getName(), level);
            levelsByValueMap.put(level.intValue(), level);
        }
        levelsByName = Collections.unmodifiableMap(levelsByNameMap);
        levelsByValue = Collections.unmodifiableMap(levelsByValueMap);

        julLevelToSyslogSeverity = Collections.unmodifiableMap(new HashMap<Level, Severity>() {
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
     * Simplification: use delegate to {@link Level#parse(String)} even if the behavior is slightly different for localized log levels.
     *
     * @param name {@code null} or empty returns {@code null}
     * @return
     */
    public static Level findLevel(String name) {
        if(name == null || name.isEmpty())
            return null;
        return Level.parse(name);

    }

    public static Severity toSeverity(Level level) {
        return julLevelToSyslogSeverity.get(level);
    }

    /**
     * Compare on {@link java.util.logging.Level#intValue()}
     */
    public static Comparator<Level> comparator() {
        return new Comparator<Level>() {
            @Override
            public int compare(Level l1, Level l2) {
                return Integer.compare(l1.intValue(), l2.intValue());
            }
        };
    }
}
