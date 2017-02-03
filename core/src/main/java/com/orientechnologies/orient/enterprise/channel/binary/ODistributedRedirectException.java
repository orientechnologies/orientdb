/*
 *
 *  * Copyright 2010-2014 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.common.exception.OSystemException;

/**
 * The operation will be redirect to another server.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedRedirectException extends OSystemException {
  private String fromServer;
  private String toServer;
  private String toServerAddress;

  public ODistributedRedirectException(final ODistributedRedirectException exception) {
    super(exception);
    this.fromServer = exception.fromServer;
    this.toServer = exception.toServer;
    this.toServerAddress = exception.toServerAddress;
  }

  public ODistributedRedirectException(final String fromServer, final String toServer, final String toServerAddress,
      final String reason) {
    super(reason);
    this.fromServer = fromServer;
    this.toServer = toServer;
    this.toServerAddress = toServerAddress;
  }

  public String getFromServer() {
    return fromServer;
  }

  public String getToServer() {
    return toServer;
  }

  public String getToServerAddress() {
    return toServerAddress;
  }

  @Override
  public String toString() {
    return getMessage() + ". Reconnecting to " + toServerAddress + " (from=" + fromServer + " to=" + toServer + ")";
  }
}
