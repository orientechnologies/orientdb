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

package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.orient.core.compression.OCompression;

/**
 * Base class for the compression implementations.
 * 
 * @author Luca Garulli
 * @since 05.06.13
 */
public abstract class OAbstractCompression implements OCompression {
  @Override
  public byte[] compress(final byte[] content) {
    return compress(content, 0, content.length);
  }

  @Override
  public byte[] uncompress(final byte[] content) {
    return uncompress(content, 0, content.length);
  }

  @Override
  public OCompression configure(final String iOptions) {
    return this;
  }
}
