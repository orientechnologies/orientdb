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
package com.orientechnologies.orient.core;

import com.orientechnologies.common.log.OLogManager;

public class OrientShutdownHook extends Thread {
  protected OrientShutdownHook() {
    try {
      Runtime.getRuntime().addShutdownHook(this);
    } catch (IllegalStateException ignore) {
      // we may be asked to initialize the runtime and install the hook from another shutdown hook
      // during the shutdown
    }
  }

  /** Shutdown Orient engine. */
  @Override
  public void run() {
    try {
      Orient.instance().shutdown();
    } finally {
      OLogManager.instance().shutdown();
    }
  }

  public void cancel() {
    try {
      Runtime.getRuntime().removeShutdownHook(this);
    } catch (IllegalStateException ignore) {
    }
  }
}
