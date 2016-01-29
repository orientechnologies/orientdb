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
package com.orientechnologies.orient.core.replication;

/**
 * Interface to catch errors on asynchronous replication.
 *
 * @author Luca Garulli
 */
public interface OAsyncReplicationError {
  enum ACTION {IGNORE, RETRY}

  /**
   * Callback called in case of error during asynchronous replication.
   *
   * @param iException The exception caught
   * @param iRetry     The number of retries so far. At every retry, this number is incremented.
   * @return RETRY to retry the operation, otherwise IGNORE
   */
  ACTION onAsyncReplicationError(Throwable iException, int iRetry);
}
