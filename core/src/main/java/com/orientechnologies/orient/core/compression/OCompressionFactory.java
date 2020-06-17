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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.compression.impl.OGZIPCompression;
import com.orientechnologies.orient.core.compression.impl.OHighZIPCompression;
import com.orientechnologies.orient.core.compression.impl.OLowZIPCompression;
import com.orientechnologies.orient.core.compression.impl.ONothingCompression;
import com.orientechnologies.orient.core.exception.OSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Factory of compression algorithms.
 *
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public class OCompressionFactory {
  public static final OCompressionFactory INSTANCE = new OCompressionFactory();

  private final Map<String, OCompression> compressions = new HashMap<String, OCompression>();
  private final Map<String, Class<? extends OCompression>> compressionClasses =
      new HashMap<String, Class<? extends OCompression>>();

  /** Install default compression algorithms. */
  public OCompressionFactory() {
    register(new OHighZIPCompression());
    register(new OLowZIPCompression());
    register(new OGZIPCompression());
    register(new ONothingCompression());
  }

  public OCompression getCompression(final String name, final String iOptions) {
    if (name.length() == 0) return ONothingCompression.INSTANCE;

    OCompression compression = compressions.get(name);
    if (compression == null) {

      final Class<? extends OCompression> compressionClass;
      if (name == null) compressionClass = ONothingCompression.class;
      else compressionClass = compressionClasses.get(name);

      if (compressionClass != null) {
        try {
          compression = compressionClass.newInstance();
          compression.configure(iOptions);

        } catch (Exception e) {
          throw OException.wrapException(
              new OSecurityException("Cannot instantiate compression algorithm '" + name + "'"), e);
        }
      } else throw new OSecurityException("Compression with name '" + name + "' is absent");
    }
    return compression;
  }

  /**
   * Registers a stateful implementations, a new instance will be created for each storage.
   *
   * @param compression Compression instance
   */
  public void register(final OCompression compression) {
    try {
      final String name = compression.name();

      if (compressions.containsKey(name))
        throw new IllegalArgumentException(
            "Compression with name '" + name + "' was already registered");

      if (compressionClasses.containsKey(name))
        throw new IllegalArgumentException(
            "Compression with name '" + name + "' was already registered");

      compressions.put(name, compression);
    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "Cannot register storage compression algorithm '%s'", e, compression);
    }
  }

  /**
   * Registers a stateless implementations, the same instance will be shared on all the storages.
   *
   * @param compression Compression class
   */
  public void register(final Class<? extends OCompression> compression) {
    try {
      final OCompression tempInstance = compression.newInstance();

      final String name = tempInstance.name();

      if (compressions.containsKey(name))
        throw new IllegalArgumentException(
            "Compression with name '" + name + "' was already registered");

      if (compressionClasses.containsKey(tempInstance.name()))
        throw new IllegalArgumentException(
            "Compression with name '" + name + "' was already registered");

      compressionClasses.put(name, compression);
    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "Cannot register storage compression algorithm '%s'", e, compression);
    }
  }

  public Set<String> getCompressions() {
    return compressions.keySet();
  }
}
