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

/**
 * Storage encryption interface. Additional encryption implementations can be plugged via <code>
 * register()</code> method. There are 2 versions:<br>
 *
 * <ul>
 *   <li><code>OEncryptionFactory.INSTANCE.register(<class>)</code> for stateful implementations, a
 *       new instance will be created for each storage/li>
 *   <li><code>OEncryptionFactory.INSTANCE.register(<instance>)</code> for stateless
 *       implementations, the same instance will be shared across all the storages./li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OEncryption {
  byte[] encrypt(byte[] content);

  byte[] decrypt(byte[] content);

  byte[] encrypt(byte[] content, final int offset, final int length);

  byte[] decrypt(byte[] content, final int offset, final int length);

  String name();

  OEncryption configure(String iOptions);
}
