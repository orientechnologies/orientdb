/*
 * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.common.factory.OConfigurableStatelessFactory;

/**
 * Factory to manage the distributed conflict resolved implementations.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedConflictResolverFactory extends OConfigurableStatelessFactory<String, ODistributedConflictResolver> {
  public ODistributedConflictResolverFactory() {
    final OQuorumDistributedConflictResolver def = new OQuorumDistributedConflictResolver();

    registerImplementation(OQuorumDistributedConflictResolver.NAME, def);
    registerImplementation(OMajorityDistributedConflictResolver.NAME, new OMajorityDistributedConflictResolver());
    registerImplementation(OContentDistributedConflictResolver.NAME, new OContentDistributedConflictResolver());
    registerImplementation(OVersionDistributedConflictResolver.NAME, new OVersionDistributedConflictResolver());
    registerImplementation(ODCDistributedConflictResolver.NAME, new ODCDistributedConflictResolver());

    setDefaultImplementation(def);
  }
}
