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

/**
 * /** Storage compression interface. Additional compression implementations can be plugged via
 * <code>register()</code> method. There are 2 versions:<br>
 *
 * <ul>
 *   <li><code>OCompressionFactory.INSTANCE.register(<class>)</code> for stateful implementations, a
 *       new instance will be created for each storage/li>
 *   <li><code>OCompressionFactory.INSTANCE.register(<instance>)</code> for stateless
 *       implementations, the same instance will be shared across all the storages./li>
 * </ul>
 *
 * @author Andrey Lomakin
 * @author Luca Garulli
 * @since 05.06.13
 */
public interface OCompression {
  byte[] compress(byte[] content);

  byte[] compress(byte[] content, final int offset, final int length);

  byte[] uncompress(byte[] content);

  byte[] uncompress(byte[] content, final int offset, final int length);

  String name();

  OCompression configure(String iOptions);
}
