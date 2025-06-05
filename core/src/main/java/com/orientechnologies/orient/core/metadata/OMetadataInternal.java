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

import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.security.OIdentity;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicy;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** Internal interface to manage metadata snapshots. */
public interface OMetadataInternal extends OMetadata {

  public static final String IDENTITY = OIdentity.CLASS_NAME.toLowerCase(Locale.ENGLISH);
  public static final String USER = OUser.CLASS_NAME.toLowerCase(Locale.ENGLISH);
  public static final String ROLE = ORole.CLASS_NAME.toLowerCase(Locale.ENGLISH);
  public static final String RESTRICTED =
      OSecurity.RESTRICTED_CLASSNAME.toLowerCase(Locale.ENGLISH);
  public static final String FUNCTION = OFunction.CLASS_NAME.toLowerCase(Locale.ENGLISH);
  public static final String SCHEDULE = OScheduledEvent.CLASS_NAME.toLowerCase(Locale.ENGLISH);
  public static final String TRIGGER = "OTrigger".toLowerCase(Locale.ENGLISH);
  public static final String POLICY =
      OSecurityPolicy.class.getName().toLowerCase().toLowerCase(Locale.ENGLISH);
  public static final String SEQUENCE =
      OSequence.CLASS_NAME.toLowerCase().toLowerCase(Locale.ENGLISH);
  // Used by the importer
  public static final String ORIDS = "ORIDs".toLowerCase().toLowerCase(Locale.ENGLISH);

  Set<String> SYSTEM_CLUSTER =
      Collections.unmodifiableSet(
          new HashSet<String>(
              Arrays.asList(
                  new String[] {
                    USER, ROLE, RESTRICTED, FUNCTION, OSessionMetadata.CLUSTER_INTERNAL_NAME,
                  })));
  Set<String> SYSTEM_CLASSES =
      Collections.unmodifiableSet(
          new HashSet<String>(
              Arrays.asList(
                  IDENTITY,
                  USER,
                  ROLE,
                  RESTRICTED,
                  FUNCTION,
                  SCHEDULE,
                  TRIGGER,
                  POLICY,
                  SEQUENCE,
                  ORIDS)));

  void makeThreadLocalSchemaSnapshot();

  void clearThreadLocalSchemaSnapshot();

  OImmutableSchema getImmutableSchemaSnapshot();

  OIndexManagerAbstract getIndexManagerInternal();
}
