/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.metadata;

import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.security.OIdentity;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OUser;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Internal interface to manage metadata snapshots.
 */
public interface OMetadataInternal extends OMetadata {

  Set<String> SYSTEM_CLUSTER = Collections.unmodifiableSet(
      new HashSet<String>(Arrays.asList(new String[] { OUser.CLASS_NAME.toLowerCase(), ORole.CLASS_NAME.toLowerCase(),
          OIdentity.CLASS_NAME.toLowerCase(), "ORIDs".toLowerCase(), OSecurity.RESTRICTED_CLASSNAME.toLowerCase(),
          "OFunction".toLowerCase(), "OTriggered".toLowerCase(), "OSchedule".toLowerCase() })));

  void makeThreadLocalSchemaSnapshot();

  void clearThreadLocalSchemaSnapshot();

  OImmutableSchema getImmutableSchemaSnapshot();

}
