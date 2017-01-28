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

  public void onDrop(ODatabase database) {
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
}
