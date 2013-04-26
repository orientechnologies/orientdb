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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class OPageId {
  public final String fileName;
  public final long   pageIndex;

  public OPageId(String fileName, long pageIndex) {
    this.fileName = fileName;
    this.pageIndex = pageIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OPageId oPageId = (OPageId) o;

    if (pageIndex != oPageId.pageIndex)
      return false;
    if (!fileName.equals(oPageId.fileName))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = fileName.hashCode();
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    return result;
  }
}
