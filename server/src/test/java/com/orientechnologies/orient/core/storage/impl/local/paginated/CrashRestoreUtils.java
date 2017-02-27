/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Sergey Sitnikov
 */
public class CrashRestoreUtils {

  private CrashRestoreUtils() {
  }

  public static void inheritIO(ProcessBuilder processBuilder) {
    try {
      final Method inheritIO = processBuilder.getClass().getMethod("inheritIO");
      inheritIO.setAccessible(true);
      inheritIO.invoke(processBuilder);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("unable to invoke inheritIO, please use 1.7+ compliant JVM");
    } catch (InvocationTargetException e) {
      throw new IllegalStateException("unable to invoke inheritIO, please use 1.7+ compliant JVM");
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("unable to invoke inheritIO, please use 1.7+ compliant JVM");
    }
  }

  public static void destroyForcibly(Process process) {
    try {
      final Method destroyForcibly = process.getClass().getMethod("destroyForcibly");
      destroyForcibly.setAccessible(true);
      destroyForcibly.invoke(process);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("unable to invoke destroyForcibly, please use 1.8+ compliant JVM");
    } catch (InvocationTargetException e) {
      throw new IllegalStateException("unable to invoke destroyForcibly, please use 1.8+ compliant JVM");
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("unable to invoke destroyForcibly, please use 1.8+ compliant JVM");
    }
  }
}
