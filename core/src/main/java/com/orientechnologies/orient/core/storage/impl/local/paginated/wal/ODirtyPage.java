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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class ODirtyPage {
  public final String              fileName;
  public final long                pageIndex;
  private final OLogSequenceNumber lsn;

  public ODirtyPage(String fileName, long pageIndex, OLogSequenceNumber lsn) {
    this.fileName = fileName;
    this.pageIndex = pageIndex;
    this.lsn = lsn;
  }

  public String getFileName() {
    return fileName;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public OLogSequenceNumber getLsn() {
    return lsn;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ODirtyPage that = (ODirtyPage) o;

    if (pageIndex != that.pageIndex)
      return false;
    if (!fileName.equals(that.fileName))
      return false;
    if (!lsn.equals(that.lsn))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = fileName.hashCode();
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + lsn.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "ODirtyPage{" + "fileName='" + fileName + '\'' + ", pageIndex=" + pageIndex + ", lsn=" + lsn + '}';
  }
}
