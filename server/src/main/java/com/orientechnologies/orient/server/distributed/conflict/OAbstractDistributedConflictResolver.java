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

package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Abstract class to simplify implementation of distributed conflict resolvers.
 *
 * @author Luca Garulli
 */
public abstract class OAbstractDistributedConflictResolver implements ODistributedConflictResolver {
  protected ODocument configuration;

  @Override
  public void configure(final ODocument config) {
    configuration = config;
  }

  public static boolean compareRecords(final ORawBuffer key1, final ORawBuffer key2) {
    boolean matched = false;
    if (Arrays.equals(key1.buffer, key2.buffer)) {
      matched = true;
    } else if (key1.recordType == ODocument.RECORD_TYPE) {
      // DOCUMENTS COULD BE THE SAME EVEN IF THE BINARY CONTENT IS NOT, COMPARING DOCUMENTS INSTEAD
      final ODocument currentDocument = new ODocument().fromStream(key1.buffer);

      if (key2.recordType == ODocument.RECORD_TYPE) {
        final ODocument otherDocument = new ODocument().fromStream(key2.buffer);
        if (currentDocument.hasSameContentOf(otherDocument)) {
          // SAME CONTENT = EQUALS
          matched = true;
        }
      }
    }
    return matched;
  }

  protected static Object getBestResult(final Map<Object, List<String>> groupedResult, final List<Object> exclude) {
    Object bestResult = NOT_FOUND;
    int max = -1;
    for (Map.Entry<Object, List<String>> entry : groupedResult.entrySet()) {
      boolean skip = false;
      if (exclude != null && !exclude.isEmpty()) {
        for (Object ex : exclude) {
          if (ex == null && entry.getKey() == null || ex != null && entry.getKey() != null && ex.equals(entry.getKey())) {
            // SKIP IT
            skip = true;
            break;
          }
        }
      }

      if (!skip)
        if (entry.getValue().size() > max) {
          bestResult = entry.getKey();
          max = entry.getValue().size();
        }
    }
    return bestResult;
  }
}
