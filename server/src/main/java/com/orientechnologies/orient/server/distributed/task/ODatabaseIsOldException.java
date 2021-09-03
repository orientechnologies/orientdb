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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.orient.server.distributed.ODistributedException;

/**
 * Exception thrown when a database is requested but it is older then the one owned by the
 * requester.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODatabaseIsOldException extends ODistributedException {
  public ODatabaseIsOldException(final ODatabaseIsOldException exception) {
    super(exception);
  }

  public ODatabaseIsOldException(final String iMessage) {
    super(iMessage);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ODatabaseIsOldException)) return false;

    return getMessage().equals(((ODatabaseIsOldException) obj).getMessage());
  }
}
