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

package com.orientechnologies.orient.core.compression;

import com.orientechnologies.orient.core.compression.impl.OGZIPCompression;
import com.orientechnologies.orient.core.compression.impl.OHighZIPCompression;
import com.orientechnologies.orient.core.compression.impl.OLowZIPCompression;
import com.orientechnologies.orient.core.compression.impl.ONothingCompression;
import com.orientechnologies.orient.core.compression.impl.OSnappyCompression;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public class OCompressionFactory {
  public static final OCompressionFactory INSTANCE     = new OCompressionFactory();

  private final Map<String, OCompression> compressions = new HashMap<String, OCompression>();

  public OCompressionFactory() {
    register(OHighZIPCompression.INSTANCE);
    register(OLowZIPCompression.INSTANCE);
    register(OGZIPCompression.INSTANCE);
    register(OSnappyCompression.INSTANCE);
    register(ONothingCompression.INSTANCE);
  }

  public OCompression getCompression(String name) {
    OCompression compression = compressions.get(name);
    if (compression == null)
      throw new IllegalArgumentException("Compression with name  " + name + " is absent.");

    return compression;
  }

  public void register(OCompression compression) {
    if (compressions.containsKey(compression.name()))
      throw new IllegalArgumentException("Compression with name " + compression.name() + " was already registered.");

    compressions.put(compression.name(), compression);
  }

  public Set<String> getCompressions() {
    return compressions.keySet();
  }
}
