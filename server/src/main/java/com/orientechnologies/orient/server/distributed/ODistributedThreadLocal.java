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
package com.orientechnologies.orient.server.distributed;

/**
 * Thread local to know when the request comes from distributed requester avoiding loops.
 * 
 * @author Luca
 * 
 */
public class ODistributedThreadLocal extends ThreadLocal<String> {
  public static ODistributedThreadLocal INSTANCE = new ODistributedThreadLocal();

  @Override
  public void set(final String value) {
    super.set(value);
  }

  @Override
  public String get() {
    return super.get();
  }
}
