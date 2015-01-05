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
package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedException;

/**
 * Exception thrown when the two servers are not aligned.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @deprecated since 2.0
 *
 */
@Deprecated
public class OReplicationConflictException extends ODistributedException {

  private static final String MESSAGE_LOCAL_VERSION    = "local=v";
  private static final String MESSAGE_ORIGINAL_VERSION = "original=v";

  private static final long   serialVersionUID         = 1L;

  private final ORID          originalRID;
  private final int           originalVersion;
  private final ORID          localRID;
  private final int           localVersion;

  /**
   * Rebuilds the original exception from the message.
   */
  public OReplicationConflictException(final String message) {
    super(message);
    int beginPos = message.indexOf(ORID.PREFIX);
    int endPos = message.indexOf(' ', beginPos);
    originalRID = new ORecordId(message.substring(beginPos, endPos));

    beginPos = message.indexOf(MESSAGE_ORIGINAL_VERSION, endPos) + MESSAGE_ORIGINAL_VERSION.length();
    endPos = message.indexOf(' ', beginPos);
    originalVersion = Integer.parseInt(message.substring(beginPos, endPos));

    beginPos = message.indexOf(MESSAGE_LOCAL_VERSION, endPos) + MESSAGE_LOCAL_VERSION.length();
    endPos = message.indexOf(')', beginPos);
    localVersion = Integer.parseInt(message.substring(beginPos, endPos));
    localRID = null;
  }

  public OReplicationConflictException(final String message, final ORID iRID, final int iDatabaseVersion, final int iRecordVersion) {
    super(message);
    originalRID = iRID;
    localRID = null;
    originalVersion = iDatabaseVersion;
    localVersion = iRecordVersion;
  }

  public OReplicationConflictException(final String message, final ORID iOriginalRID, final ORID iLocalRID) {
    super(message);
    originalRID = iOriginalRID;
    localRID = iLocalRID;
    originalVersion = localVersion = 0;
  }

  @Override
  public String getMessage() {
    final StringBuilder buffer = new StringBuilder(super.getMessage());

    if (localRID != null) {
      // RID CONFLICT
      buffer.append("original RID=");
      buffer.append(originalRID);
      buffer.append(" local RID=");
      buffer.append(localRID);
    } else {
      // VERSION CONFLICT
      buffer.append(MESSAGE_ORIGINAL_VERSION);
      buffer.append(originalVersion);
      buffer.append(' ');
      buffer.append(MESSAGE_LOCAL_VERSION);
      buffer.append(localVersion);
    }

    return buffer.toString();
  }

  @Override
  public String toString() {
    return getMessage();
  }

  public int getOriginalVersion() {
    return originalVersion;
  }

  public int getLocalVersion() {
    return localVersion;
  }

  public ORID getOriginalRID() {
    return originalRID;
  }

  public ORID getLocalRID() {
    return localRID;
  }
}
