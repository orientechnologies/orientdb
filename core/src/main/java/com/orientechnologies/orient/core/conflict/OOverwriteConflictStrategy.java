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

package com.orientechnologies.orient.core.conflict;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Auto merges new record with the existent. Collections are also merged, item by item.
 *
 * @author Luca Garulli
 */
public class OOverwriteConflictStrategy extends OVersionRecordConflictStrategy {
  public static final String NAME = "overwrite";

  @Override
  public byte[] onUpdate(OStorage storage, byte iRecordType, final ORecordId rid, final int iRecordVersion,
      final byte[] iRecordContent, final AtomicInteger iDatabaseVersion) {

    iDatabaseVersion.set(Math.max(iDatabaseVersion.get(), iRecordVersion) + 1);
    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
