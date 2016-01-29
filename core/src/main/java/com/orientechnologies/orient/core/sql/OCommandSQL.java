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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandRequestTextAbstract;
import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.replication.OAsyncReplicationOk;

/**
 * SQL command request implementation. It just stores the request and delegated the execution to the configured OCommandExecutor.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("serial")
public class OCommandSQL extends OCommandRequestTextAbstract {
  public OCommandSQL() {
  }

  public OCommandSQL(final String iText) {
    super(iText);
  }

  public boolean isIdempotent() {
    return false;
  }

  @Override
  public String toString() {
    return "sql." + text;// OIOUtils.getStringMaxLength(text, 50, "...");
  }

  /**
   * Defines a callback to call in case of the asynchronous replication succeed.
   */
  @Override
  public OCommandSQL onAsyncReplicationOk(final OAsyncReplicationOk iCallback) {
    return (OCommandSQL) super.onAsyncReplicationOk(iCallback);
  }

  /**
   * Defines a callback to call in case of error during the asynchronous replication.
   */
  @Override
  public OCommandSQL onAsyncReplicationError(final OAsyncReplicationError iCallback) {
    return (OCommandSQL) super.onAsyncReplicationError(iCallback);
  }
}
