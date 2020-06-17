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

package com.orientechnologies.orient.core.storage.cache;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 25.04.13
 */
public class OPageDataVerificationError {
  private final boolean incorrectMagicNumber;
  private final boolean incorrectCheckSum;
  private final long pageIndex;
  private final String fileName;

  public OPageDataVerificationError(
      boolean incorrectMagicNumber, boolean incorrectCheckSum, long pageIndex, String fileName) {
    this.incorrectMagicNumber = incorrectMagicNumber;
    this.incorrectCheckSum = incorrectCheckSum;
    this.pageIndex = pageIndex;
    this.fileName = fileName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OPageDataVerificationError that = (OPageDataVerificationError) o;

    if (incorrectCheckSum != that.incorrectCheckSum) return false;
    if (incorrectMagicNumber != that.incorrectMagicNumber) return false;
    if (pageIndex != that.pageIndex) return false;
    if (!fileName.equals(that.fileName)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (incorrectMagicNumber ? 1 : 0);
    result = 31 * result + (incorrectCheckSum ? 1 : 0);
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + fileName.hashCode();
    return result;
  }
}
