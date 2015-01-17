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

package com.orientechnologies.orient.core.serialization.serializer;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

public class ONetworkThreadLocalSerializer {
  private static volatile ThreadLocal<ORecordSerializer> networkSerializer = new ThreadLocal<ORecordSerializer>();

  public static ORecordSerializer getNetworkSerializer() {
    return networkSerializer != null ? networkSerializer.get() : null;
  }

  public static void setNetworkSerializer(ORecordSerializer value) {
    networkSerializer.set(value);
  }

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (networkSerializer == null)
          networkSerializer = new ThreadLocal<ORecordSerializer>();
      }

      @Override
      public void onShutdown() {
        networkSerializer = null;
      }
    });
  }
}
