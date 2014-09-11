/*
 * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.conflict;

import com.orientechnologies.common.factory.OConfigurableStatefulFactory;

/**
 * Factory to manage the record conflict strategy implementations.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ORecordConflictStrategyFactory extends OConfigurableStatefulFactory<String, ORecordConflictStrategy> {
  public ORecordConflictStrategyFactory() {
    setDefaultClass(OVersionRecordConflictStrategy.class);

    register(OVersionRecordConflictStrategy.NAME, OVersionRecordConflictStrategy.class);
    register(OAutoMergeRecordConflictStrategy.NAME, OAutoMergeRecordConflictStrategy.class);
    register(OContentRecordConflictStrategy.NAME, OContentRecordConflictStrategy.class);
  }

  public ORecordConflictStrategy getStrategy(final String iStrategy) {
    return newInstance(iStrategy);
  }

  public String getDefaultStrategy() {
    return OVersionRecordConflictStrategy.NAME;
  }
}
