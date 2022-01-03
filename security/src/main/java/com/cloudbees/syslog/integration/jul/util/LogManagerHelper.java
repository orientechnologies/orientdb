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

import java.util.logging.Filter;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogManager;

/** @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a> */
public class LogManagerHelper {

  /**
   * Visible version of {@link java.util.logging.LogManager#getLevelProperty(String,
   * java.util.logging.Level)}.
   *
   * <p>If the property is not defined or cannot be parsed we return the given default value.
   */
  public static Level getLevelProperty(LogManager manager, String name, Level defaultValue) {

    String val = manager.getProperty(name);
    if (val == null) {
      return defaultValue;
    }
    Level l = LevelHelper.findLevel(val.trim());
    return l != null ? l : defaultValue;
  }

  /**
   * Visible version of {@link java.util.logging.LogManager#getFilterProperty(String,
   * java.util.logging.Filter)}.
   *
   * <p>We return an instance of the class named by the "name" property.
   *
   * <p>If the property is not defined or has problems we return the defaultValue.
   */
  public static Filter getFilterProperty(LogManager manager, String name, Filter defaultValue) {
    String val = manager.getProperty(name);
    try {
      if (val != null) {
        Class clz = ClassLoader.getSystemClassLoader().loadClass(val);
        return (Filter) clz.newInstance();
      }
    } catch (Exception ex) {
      // We got one of a variety of exceptions in creating the
      // class or creating an instance.
      // Drop through.
    }
    // We got an exception.  Return the defaultValue.
    return defaultValue;
  }

  /**
   * Visible version of {@link java.util.logging.LogManager#getFormatterProperty(String,
   * java.util.logging.Formatter)} .
   *
   * <p>We return an instance of the class named by the "name" property.
   *
   * <p>If the property is not defined or has problems we return the defaultValue.
   */
  public static Formatter getFormatterProperty(
      LogManager manager, String name, Formatter defaultValue) {
    String val = manager.getProperty(name);
    try {
      if (val != null) {
        Class clz = ClassLoader.getSystemClassLoader().loadClass(val);
        return (Formatter) clz.newInstance();
      }
    } catch (Exception ex) {
      // We got one of a variety of exceptions in creating the
      // class or creating an instance.
      // Drop through.
    }
    // We got an exception.  Return the defaultValue.
    return defaultValue;
  }

  /**
   * Visible version of {@link java.util.logging.LogManager#getStringProperty(String, String)}.
   *
   * <p>If the property is not defined we return the given default value.
   */
  public static String getStringProperty(LogManager manager, String name, String defaultValue) {
    String val = manager.getProperty(name);
    if (val == null) {
      return defaultValue;
    }
    return val.trim();
  }

  /**
   * Visible version of {@link java.util.logging.LogManager#getIntProperty(String, int)}.
   *
   * <p>Method to get an integer property.
   *
   * <p>If the property is not defined or cannot be parsed we return the given default value.
   */
  public static int getIntProperty(LogManager manager, String name, int defaultValue) {
    String val = manager.getProperty(name);
    if (val == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(val.trim());
    } catch (Exception ex) {
      return defaultValue;
    }
  }

  /**
   * Visible version of {@link java.util.logging.LogManager#getIntProperty(String, int)}.
   *
   * <p>Method to get a boolean property.
   *
   * <p>If the property is not defined or cannot be parsed we return the given default value.
   */
  public static boolean getBooleanProperty(LogManager manager, String name, boolean defaultValue) {
    String val = manager.getProperty(name);
    if (val == null) {
      return defaultValue;
    }
    val = val.toLowerCase();
    if (val.equals("true") || val.equals("1")) {
      return true;
    } else if (val.equals("false") || val.equals("0")) {
      return false;
    }
    return defaultValue;
  }
}
