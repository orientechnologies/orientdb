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

package com.orientechnologies.orient.core.serialization.serializer.record;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Thread local set of serialized documents. Used to prevent infinite recursion during serialization of records.
 * 
 * @author Artem Loginov (logart2007-at-gmail.com), Artem Orobets (enisher-at-gmail.com)
 */
public class OSerializationSetThreadLocal extends ThreadLocal<Map<ODocument, Boolean>> {
  public static OSerializationSetThreadLocal INSTANCE = new OSerializationSetThreadLocal();

  public static boolean check(final ODocument document) {
    return INSTANCE.get().containsKey(document);
  }

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (INSTANCE == null)
          INSTANCE = new OSerializationSetThreadLocal();
      }

      @Override
      public void onShutdown() {
        INSTANCE = null;
      }
    });
  }

  public static boolean checkIfPartial(final ODocument document) {
    final Boolean partial = INSTANCE.get().get(document);
    if (partial != null) {
      // if (partial)
      // // REMOVE FROM SET TO LET SERIALIZE IT AT UPPER LEVEL
      // INSTANCE.get().remove(document);
      return partial;
    }
    return false;
  }

  public static void setPartial(final ODocument document) {
    INSTANCE.get().put(document, Boolean.TRUE);
  }

  public static boolean checkAndAdd(final ODocument document) {
    final Map<ODocument, Boolean> iMarshalledRecords = INSTANCE.get();
    // CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
    if (iMarshalledRecords.containsKey(document))
      return false;

    iMarshalledRecords.put(document, Boolean.FALSE);
    return true;
  }

  public static void clear() {
    INSTANCE.get().clear();
  }

  public static void removeCheck(final ODocument document) {
    INSTANCE.get().remove(document);
  }

  @Override
  protected Map<ODocument, Boolean> initialValue() {
    return new IdentityHashMap<ODocument, Boolean>();
  }

}
