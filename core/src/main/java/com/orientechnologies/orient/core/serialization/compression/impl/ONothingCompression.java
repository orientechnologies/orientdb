/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.serialization.compression.impl;

import com.orientechnologies.orient.core.serialization.compression.OCompression;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public class ONothingCompression implements OCompression {
  public static final String              NAME     = "nothing";

  public static final ONothingCompression INSTANCE = new ONothingCompression();

  @Override
  public byte[] compress(byte[] content) {
    byte[] result = new byte[content.length];
    System.arraycopy(content, 0, result, 0, content.length);

    return result;
  }

  @Override
  public byte[] uncompress(byte[] content) {
    byte[] result = new byte[content.length];
    System.arraycopy(content, 0, result, 0, content.length);

    return result;
  }

  @Override
  public String name() {
    return NAME;
  }
}
