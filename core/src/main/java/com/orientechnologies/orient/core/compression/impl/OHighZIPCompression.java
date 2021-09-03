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

import java.util.zip.ZipOutputStream;

/**
 * Compression implementation that use ZIP algorithm to the maximum level of compression
 *
 * @author Luca Garulli
 */
public class OHighZIPCompression extends OZIPCompression {
  public static final OHighZIPCompression INSTANCE = new OHighZIPCompression();
  public static final String NAME = "high-zip";

  @Override
  public String name() {
    return NAME;
  }

  protected void setLevel(ZipOutputStream zipOutputStream) {
    zipOutputStream.setLevel(9);
  }
}
