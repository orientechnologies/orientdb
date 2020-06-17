/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Listener Interface to receive callbacks on database usage.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface ODatabaseLifecycleListener {

  enum PRIORITY {
    FIRST,
    EARLY,
    REGULAR,
    LATE,
    LAST
  }

  default PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  void onCreate(ODatabaseInternal iDatabase);

  void onOpen(ODatabaseInternal iDatabase);

  void onClose(ODatabaseInternal iDatabase);

  void onDrop(ODatabaseInternal iDatabase);

  @Deprecated
  default void onCreateClass(ODatabaseInternal iDatabase, OClass iClass) {}

  @Deprecated
  default void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {}

  default void onCreateView(ODatabaseInternal database, OView view) {}

  default void onDropView(ODatabaseInternal database, OView cls) {}

  /**
   * Event called during the retrieving of distributed configuration, usually at startup and when
   * the cluster shape changes. You can use this event to enrich the ODocument sent to the client
   * with custom properties.
   *
   * @param iConfiguration
   */
  void onLocalNodeConfigurationRequest(ODocument iConfiguration);
}
