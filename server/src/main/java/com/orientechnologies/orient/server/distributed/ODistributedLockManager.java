/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

/**
 * Distributed Lock Manager interface.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public interface ODistributedLockManager {

  void acquireExclusiveLock(final String resource, final String nodeSource, final long timeout);

  void releaseExclusiveLock(final String resource, final String nodeSource);

  void handleUnreachableServer(final String nodeLeftName);

  void shutdown();
}
