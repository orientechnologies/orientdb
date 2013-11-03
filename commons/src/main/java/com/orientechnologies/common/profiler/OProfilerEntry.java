/*
 * Copyright 1999-2005 Luca Garulli (l.garulli--at-orientechnologies.com)
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
package com.orientechnologies.common.profiler;

public class OProfilerEntry {
  public String name    = null;
  public long   entries = 0;
  public long   last    = 0;
  public long   min     = 999999999;
  public long   max     = 0;
  public long   average = 0;
  public long   total   = 0;
  public String payLoad;
  public String description;

  public String toJSON() {
    final StringBuilder buffer = new StringBuilder();
    toJSON(buffer);
    return buffer.toString();
  }

  public void toJSON(final StringBuilder buffer) {
    buffer.append('{');
    buffer.append(String.format("\"%s\":%d,", "entries", entries));
    buffer.append(String.format("\"%s\":%d,", "last", last));
    buffer.append(String.format("\"%s\":%d,", "min", min));
    buffer.append(String.format("\"%s\":%d,", "max", max));
    buffer.append(String.format("\"%s\":%d,", "average", average));
    buffer.append(String.format("\"%s\":%d", "total", total));
    if (payLoad != null)
      buffer.append(String.format("\"%s\":%d", "payload", payLoad));
    buffer.append('}');
  }

  @Override
  public String toString() {
    return String.format("Profiler entry [%s]: total=%d, average=%d, items=%d, last=%d, max=%d, min=%d", total, name, average,
        entries, last, max, min);
  }
}