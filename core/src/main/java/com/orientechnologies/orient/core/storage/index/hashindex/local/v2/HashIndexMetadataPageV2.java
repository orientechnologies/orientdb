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

package com.orientechnologies.orient.core.storage.index.hashindex.local.v2;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/8/14
 */
public final class HashIndexMetadataPageV2 extends ODurablePage {
  private static final int RECORDS_COUNT_OFFSET = NEXT_FREE_POSITION;

  public HashIndexMetadataPageV2(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setLongValue(RECORDS_COUNT_OFFSET, 0);
  }

  public void setRecordsCount(long recordsCount) {
    setLongValue(RECORDS_COUNT_OFFSET, recordsCount);
  }

  public long getRecordsCount() {
    return getLongValue(RECORDS_COUNT_OFFSET);
  }
}
