/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.Collection;
import java.util.Set;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;

/**
 * Server cluster interface to abstract cluster behavior.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OServerCluster {

  public Object executeOperation(final String iNodeId, final byte op, final String dbName, final ORecordId rid, final int iVersion,
      final ORawBuffer record) throws ODistributedException;

  public Collection<Object> executeOperation(final Set<String> iNodeIds, final byte op, final String dbName, final ORecordId rid,
      final int iVersion, final ORawBuffer record) throws ODistributedException;

  public String getLocalNodeId();

  public Set<String> getRemoteNodeIds();

  public ODocument getDatabaseConfiguration(final String iDatabaseName);

  public ODocument getServerDatabaseConfiguration(final String iDatabaseName);

  public ODocument getClusterConfiguration();
}
