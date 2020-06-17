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
package com.orientechnologies.orient.console;

import com.orientechnologies.common.console.OConsoleApplication;

public abstract class OrientConsole extends OConsoleApplication
    implements OTableFormatter.OTableOutput {

  public OrientConsole(String[] args) {
    super(args);
  }

  @Override
  public void onMessage(String text, Object... args) {
    message(text, args);
  }

  @Override
  protected void onException(Throwable e) {
    Throwable current = e;
    while (current != null) {
      err.print("\nError: " + current.toString() + "\n");
      current = current.getCause();
    }
  }

  @Override
  protected void onBefore() {
    printApplicationInfo();
  }

  protected void printApplicationInfo() {}

  @Override
  protected void onAfter() {
    out.println();
  }

  protected String format(final String iValue, final int iMaxSize) {
    if (iValue == null) return null;

    if (iValue.length() > iMaxSize) return iValue.substring(0, iMaxSize - 3) + "...";
    return iValue;
  }

  public boolean historyEnabled() {
    for (String arg : args) {
      if (arg.equalsIgnoreCase(PARAM_DISABLE_HISTORY)) {
        return false;
      }
    }
    return true;
  }
}
