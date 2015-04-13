/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.console;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;

public class OConsoleDatabaseListener implements ODatabaseListener {
  OConsoleDatabaseApp console;

  public OConsoleDatabaseListener(OConsoleDatabaseApp console) {
    this.console = console;
  }

  public void onCreate(ODatabase iDatabase) {
  }

  public void onDelete(ODatabase iDatabase) {
  }

  public void onOpen(ODatabase iDatabase) {
  }

  public void onBeforeTxBegin(ODatabase iDatabase) {
  }

  public void onBeforeTxRollback(ODatabase iDatabase) {
  }

  public void onAfterTxRollback(ODatabase iDatabase) {
  }

  public void onBeforeTxCommit(ODatabase iDatabase) {
  }

  public void onAfterTxCommit(ODatabase iDatabase) {
  }

  public void onClose(ODatabase iDatabase) {
  }

  @Override
  public void onBeforeCommand(OCommandRequestText iCommand, OCommandExecutor executor) {

  }

  @Override
  public void onAfterCommand(OCommandRequestText iCommand, OCommandExecutor executor, Object result) {

  }

  public boolean onCorruptionRepairDatabase(ODatabase iDatabase, final String iProblem, String iWhatWillbeFixed) {
    final String answer = console.ask("\nDatabase seems corrupted:\n> " + iProblem + "\nAuto-repair will execute this action:\n> "
        + iWhatWillbeFixed + "\n\nDo you want to repair it (Y/n)? ");
    return answer.length() == 0 || answer.equalsIgnoreCase("Y") || answer.equalsIgnoreCase("Yes");
  }
}
