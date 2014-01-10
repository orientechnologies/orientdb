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
 * Abstract WAL record.
 * 
 * @author Luca Garulli
 * @since 12.12.13
 */
public abstract class OAbstractWALRecord implements OWALRecord {
  protected OLogSequenceNumber lsn;

  protected OAbstractWALRecord() {
  }

  protected OAbstractWALRecord(final OLogSequenceNumber previousCheckpoint) {
    this.lsn = previousCheckpoint;
  }

  public OLogSequenceNumber getLsn() {
    return lsn;
  }

  public void setLsn(final OLogSequenceNumber lsn) {
    this.lsn = lsn;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OAbstractWALRecord that = (OAbstractWALRecord) o;

    if (lsn != null ? !lsn.equals(that.lsn) : that.lsn != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return lsn != null ? lsn.hashCode() : 0;
  }

  @Override
  public String toString() {
    return toString(null);
  }

  protected String toString(final String iToAppend) {
    final StringBuilder buffer = new StringBuilder(getClass().getName());
    buffer.append("{lsn=").append(lsn);
    if (iToAppend != null) {
      buffer.append(", ");
      buffer.append(iToAppend);
    }
    buffer.append('}');
    return buffer.toString();
  }
}
