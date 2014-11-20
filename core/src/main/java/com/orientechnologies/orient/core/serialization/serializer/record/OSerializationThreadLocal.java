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

import com.orientechnologies.orient.core.OShutdownListener;
import com.orientechnologies.orient.core.Orient;

import java.util.HashSet;
import java.util.Set;

public class OSerializationThreadLocal extends ThreadLocal<Set<Integer>> {
  public static volatile OSerializationThreadLocal INSTANCE = new OSerializationThreadLocal();

  static {
    Orient.instance().addShutdownListener(new OShutdownListener() {
      @Override
      public void onShutdown() {
        INSTANCE = null;
      }
    });
  }

  @Override
  protected Set<Integer> initialValue() {
    return new HashSet<Integer>();
  }
}
