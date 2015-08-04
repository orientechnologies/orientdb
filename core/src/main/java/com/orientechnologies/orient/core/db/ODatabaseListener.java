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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;

/**
 * Listener Interface for all the events of the Database instances.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface ODatabaseListener {

  void onCreate(final ODatabase iDatabase);

  void onDelete(final ODatabase iDatabase);

  void onOpen(final ODatabase iDatabase);

  void onBeforeTxBegin(final ODatabase iDatabase);

  void onBeforeTxRollback(final ODatabase iDatabase);

  void onAfterTxRollback(final ODatabase iDatabase);

  void onBeforeTxCommit(final ODatabase iDatabase);

  void onAfterTxCommit(final ODatabase iDatabase);

  void onClose(final ODatabase iDatabase);

  void onBeforeCommand(final OCommandRequestText iCommand, final OCommandExecutor executor);

  void onAfterCommand(final OCommandRequestText iCommand, final OCommandExecutor executor, Object result);

  /**
   * Callback to decide if repair the database upon corruption.
   * 
   * @param iDatabase
   *          Target database
   * @param iReason
   *          Reason of corruption
   * @param iWhatWillbeFixed
   *          TODO
   * @return true if repair must be done, otherwise false
   */
  boolean onCorruptionRepairDatabase(final ODatabase iDatabase, final String iReason, String iWhatWillbeFixed);
}
