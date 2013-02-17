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
package com.orientechnologies.orient.core.storage.impl.local.eh;

import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;

/**
 * @author Andrey Lomakin
 * @since 06.02.13
 */
public class OEHFileMetadata {
  public static final String DEF_EXTENSION     = ".oef";

  private OSingleFileSegment file;
  private long               bucketsCount;
  private long               tombstonePosition = -1;

  public OSingleFileSegment getFile() {
    return file;
  }

  public void setFile(OSingleFileSegment file) {
    this.file = file;
  }

  public long geBucketsCount() {
    return bucketsCount;
  }

  public void setBucketsCount(long recordsCount) {
    this.bucketsCount = recordsCount;
  }

  public long getTombstonePosition() {
    return tombstonePosition;
  }

  public void setTombstonePosition(long tombstonePosition) {
    this.tombstonePosition = tombstonePosition;
  }

  public void rename(String iOldName, String iNewName) {
    file.rename(iOldName, iNewName);
  }
}
