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
package com.orientechnologies.orient.core.hook;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.HashSet;
import java.util.Set;

/**
 * Avoid recursion when hooks call themselves on the same record instances.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHookThreadLocal extends ThreadLocal<Set<OIdentifiable>> {
  public static volatile OHookThreadLocal INSTANCE = new OHookThreadLocal();

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (INSTANCE == null)
          INSTANCE = new OHookThreadLocal();
      }

      @Override
      public void onShutdown() {
        INSTANCE = null;
      }
    });
  }

  public boolean push(final OIdentifiable iRecord) {
    final Set<OIdentifiable> set = get();
    if (set.contains(iRecord))
      return false;

    set.add(iRecord);
    return true;
  }

  public boolean pop(final OIdentifiable iRecord) {
    return get().remove(iRecord);
  }

  @Override
  protected Set<OIdentifiable> initialValue() {
    return new HashSet<OIdentifiable>();
  }
}
