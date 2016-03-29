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
package com.cloudbees.syslog.integration.jul;

import com.cloudbees.syslog.integration.jul.util.LogManagerHelper;

import java.util.logging.*;

/**
 * @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a>
 */
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
        this.formatter = LogManagerHelper.getFormatterProperty(manager, cname + ".formatter", getDefaultFormatter());
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


    /**
     * {@inheritDoc}
     */
    public boolean isLoggable(LogRecord record) {
        if (record == null) {
            return false;
        }
        return super.isLoggable(record);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Level getLevel() {
        return this.logLevel;
    }

    /**
     * {@inheritDoc}
     */
    public Filter getFilter() {
        return filter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Formatter getFormatter() {
        return formatter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFormatter(Formatter formatter) throws SecurityException {
        this.formatter = formatter;
    }
}
