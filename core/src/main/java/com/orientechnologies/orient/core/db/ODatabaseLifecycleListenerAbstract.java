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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Abstract Listener Interface to receive callbacks on database usage.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public abstract class ODatabaseLifecycleListenerAbstract implements ODatabaseLifecycleListener {

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.REGULAR;
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {}

  @Override
  public void onOpen(ODatabaseInternal iDatabase) {}

  @Override
  public void onClose(ODatabaseInternal iDatabase) {}

  @Override
  public void onDrop(ODatabaseInternal iDatabase) {}

  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {}
}
