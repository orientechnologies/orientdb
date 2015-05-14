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

package com.orientechnologies.orient.core.storage.cache;

import java.security.SecureRandom;

public class OSnowFlakeIdGen implements OWriteCacheIdGen {
  private long               lastTs;
  private final SecureRandom rnd;
  private int                sequenceCounter = 1;
  private byte[]             rndBytes        = new byte[2];

  public OSnowFlakeIdGen() {
    rnd = new SecureRandom();
  }

  @Override
  public synchronized int nextId() {
    int id;
    if (sequenceCounter < 15)
      sequenceCounter++;
    else
      sequenceCounter = 1;

    lastTs = System.currentTimeMillis();
    rnd.nextBytes(rndBytes);
    id = composeId();
    // This id is used for generate fileId that in case of ridBag cannot be negative.
    if (id < 0)
      id *= -1;
    return id;
  }

  private int composeId() {
    return ((rndBytes[0] & 0xFF) << 24) | ((rndBytes[1] & 0xFF) << 16) | (((int) (lastTs & 0xFFF)) << 4) | (sequenceCounter & 0xF);
  }
}
