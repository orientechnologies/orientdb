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

package com.orientechnologies.common.thread;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OService;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;

public abstract class OSoftThread extends Thread implements OService {
  private volatile boolean shutdownFlag;

  private boolean dumpExceptions = true;

  public OSoftThread(String name) {
    super(name);
    setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
  }

  public OSoftThread(final ThreadGroup group, final String name) {
    super(group, name);
    setDaemon(true);
    setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
  }

  protected abstract void execute() throws Exception;

  public void startup() {}

  public void shutdown() {}

  public void sendShutdown() {
    shutdownFlag = true;
  }

  public void softShutdown() {
    shutdownFlag = true;
  }

  public boolean isShutdownFlag() {
    return shutdownFlag;
  }

  @Override
  public void run() {
    startup();

    while (!shutdownFlag && !isInterrupted()) {
      try {
        beforeExecution();
        execute();
        afterExecution();
      } catch (Exception e) {
        if (dumpExceptions) OLogManager.instance().error(this, "Error during thread execution", e);
      } catch (Error e) {
        if (dumpExceptions) OLogManager.instance().error(this, "Error during thread execution", e);
        shutdown();
        throw e;
      }
    }

    shutdown();
  }

  public void setDumpExceptions(final boolean dumpExceptions) {
    this.dumpExceptions = dumpExceptions;
  }

  protected void beforeExecution() throws InterruptedException {
    return;
  }

  protected void afterExecution() throws InterruptedException {
    return;
  }
}
