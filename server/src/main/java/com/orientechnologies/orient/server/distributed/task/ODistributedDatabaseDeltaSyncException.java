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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.distributed.ODistributedException;

/**
 * Exception thrown when a delta backup is not possible.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedDatabaseDeltaSyncException extends ODistributedException {
  public ODistributedDatabaseDeltaSyncException(final ODistributedDatabaseDeltaSyncException exception) {
    super(exception);
  }

  public ODistributedDatabaseDeltaSyncException(final OLogSequenceNumber requested) {
    super("Requested database delta sync with LSN=" + requested + " but not found in database");
  }

  public ODistributedDatabaseDeltaSyncException(final String iMessage) {
    super(iMessage);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ODistributedDatabaseDeltaSyncException))
      return false;

    return getMessage().equals(((ODistributedDatabaseDeltaSyncException) obj).getMessage());
  }
}
