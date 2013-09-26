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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord;

/**
 * @author Andrey Lomakin
 * @since 27.05.13
 */
public abstract class OFullPageDiff<T> extends OPageDiff<T> {
  protected T oldValue;

  public OFullPageDiff() {
  }

  public OFullPageDiff(T newValue, int pageOffset, T oldValue) {
    super(newValue, pageOffset);
    this.oldValue = oldValue;
  }

  public abstract void revertPageData(long pagePointer);

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    OFullPageDiff that = (OFullPageDiff) o;

    if (!oldValue.equals(that.oldValue))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + oldValue.hashCode();
    return result;
  }
}
