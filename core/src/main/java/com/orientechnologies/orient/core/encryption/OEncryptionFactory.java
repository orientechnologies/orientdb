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

package com.orientechnologies.orient.core.encryption;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.encryption.impl.OAESEncryption;
import com.orientechnologies.orient.core.encryption.impl.OAESGCMEncryption;
import com.orientechnologies.orient.core.encryption.impl.ODESEncryption;
import com.orientechnologies.orient.core.encryption.impl.ONothingEncryption;
import com.orientechnologies.orient.core.exception.OSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Factory of encryption algorithms.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OEncryptionFactory {
  public static final OEncryptionFactory INSTANCE = new OEncryptionFactory();

  private final Map<String, OEncryption> instances = new HashMap<String, OEncryption>();
  private final Map<String, Class<? extends OEncryption>> classes =
      new HashMap<String, Class<? extends OEncryption>>();

  /** Install default encryption algorithms. */
  public OEncryptionFactory() {
    register(ONothingEncryption.class);
    register(ODESEncryption.class);
    register(OAESEncryption.class);
    register(OAESGCMEncryption.class);
  }

  public OEncryption getEncryption(final String name, final String iOptions) {
    OEncryption encryption = instances.get(name);
    if (encryption == null) {

      final Class<? extends OEncryption> encryptionClass;

      if (name == null) encryptionClass = ONothingEncryption.class;
      else encryptionClass = classes.get(name);

      if (encryptionClass != null) {
        try {
          encryption = encryptionClass.newInstance();
          encryption.configure(iOptions);

        } catch (Exception e) {
          throw OException.wrapException(
              new OSecurityException("Cannot instantiate encryption algorithm '" + name + "'"), e);
        }
      } else throw new OSecurityException("Encryption with name '" + name + "' is absent");
    }
    return encryption;
  }

  /**
   * Registers a stateful implementations, a new instance will be created for each storage.
   *
   * @param iEncryption Encryption instance
   */
  public void register(final OEncryption iEncryption) {
    try {
      final String name = iEncryption.name();

      if (instances.containsKey(name))
        throw new IllegalArgumentException(
            "Encryption with name '" + name + "' was already registered");

      if (classes.containsKey(name))
        throw new IllegalArgumentException(
            "Encryption with name '" + name + "' was already registered");

      instances.put(name, iEncryption);
    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "Cannot register storage encryption algorithm '%s'", e, iEncryption);
    }
  }

  /**
   * Registers a stateless implementations, the same instance will be shared on all the storages.
   *
   * @param iEncryption Encryption class
   */
  public void register(final Class<? extends OEncryption> iEncryption) {
    try {
      final OEncryption tempInstance = iEncryption.newInstance();

      final String name = tempInstance.name();

      if (instances.containsKey(name))
        throw new IllegalArgumentException(
            "Encryption with name '" + name + "' was already registered");

      if (classes.containsKey(tempInstance.name()))
        throw new IllegalArgumentException(
            "Encryption with name '" + name + "' was already registered");

      classes.put(name, iEncryption);
    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "Cannot register storage encryption algorithm '%s'", e, iEncryption);
    }
  }

  public Set<String> getInstances() {
    return instances.keySet();
  }
}
