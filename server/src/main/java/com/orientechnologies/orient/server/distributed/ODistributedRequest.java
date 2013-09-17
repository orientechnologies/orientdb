/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import java.io.Serializable;

/**
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface ODistributedRequest {
  Object getId();

  ODistributedRequest setId(final Object id);

  String getDatabaseName();

  ODistributedRequest setDatabaseName(final String databaseName);

  String getClusterName();

  ODistributedRequest setClusterName(final String clusterName);

  String getSenderNodeName();

  long getSenderThreadId();

  ODistributedRequest setSenderThreadId(final long threadId);

  Serializable getPayload();

  ODistributedRequest setPayload(final Serializable payload);
}
