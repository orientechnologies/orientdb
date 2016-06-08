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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;

import java.util.Set;

/**
 * Interface to manage balancing of cluster ownership.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public interface OClusterOwnershipAssignmentStrategy {

  boolean assignClusterOwnershipOfClass(final ODatabaseInternal iDatabase, final ODistributedConfiguration cfg, final OClass iClass,
      final Set<String> availableNodes, final Set<String> clustersToReassign, boolean rebalance);

}
