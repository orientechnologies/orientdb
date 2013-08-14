/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.serialization.serializer.record;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Thread local set of serialized documents. Used to prevent infinite recursion during serialization of records.
 * 
 * @author Artem Loginov (logart2007@gmail.com), Artem Orobets (enisher@gmail.com)
 */
public class OSerializationSetThreadLocal extends ThreadLocal<Set<ODocument>> {
  public static OSerializationSetThreadLocal INSTANCE = new OSerializationSetThreadLocal();

  @Override
  protected Set<ODocument> initialValue() {
    return Collections.newSetFromMap(new IdentityHashMap<ODocument, Boolean>());
  }
}
