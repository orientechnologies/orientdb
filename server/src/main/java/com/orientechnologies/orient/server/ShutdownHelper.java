/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Executes a JVM shutdown if the settings allow it.
 */
public class ShutdownHelper {
  public static void shutdown(final int code) {
    boolean allowed = OGlobalConfiguration.ENVIRONMENT_ALLOW_JVM_SHUTDOWN.getValueAsBoolean();
    if (allowed) {
      System.exit(code);
    } else {
      throw new Error("Shutdown forbidden by configuration (" + OGlobalConfiguration.ENVIRONMENT_ALLOW_JVM_SHUTDOWN.getKey()
          + "), code: " + code);
    }
  }
}
