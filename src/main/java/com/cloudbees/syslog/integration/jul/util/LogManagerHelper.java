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

import java.util.logging.Filter;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a>
 */
public class LogManagerHelper {

    /**
     * Visible version of {@link java.util.logging.LogManager#getLevelProperty(String, java.util.logging.Level)}.
     *
     * If the property is not defined or cannot be parsed we return the given default value.
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
     * Visible version of {@link java.util.logging.LogManager#getFilterProperty(String, java.util.logging.Filter)}.
     *
     * We return an instance of the class named by the "name" property.
     *
     * If the property is not defined or has problems we return the defaultValue.
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
     * Visible version of {@link java.util.logging.LogManager#getFormatterProperty(String, java.util.logging.Formatter)} .
     *
     * We return an instance of the class named by the "name" property.
     *
     * If the property is not defined or has problems we return the defaultValue.
     */
    public static Formatter getFormatterProperty(LogManager manager, String name, Formatter defaultValue) {
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
     * If the property is not defined we return the given default value.
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
     * Method to get an integer property.
     *
     * If the property is not defined or cannot be parsed we return the given default value.
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
     * Method to get a boolean property.
     *
     * If the property is not defined or cannot be parsed we return the given default value.
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
