/*
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
package com.orientechnologies.orient.object.enhancement;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Wraps a {@link OObjectFieldHandlingStrategy} to be accessed statically.<br/>
 * The strategy to be used is set via {@link OGlobalConfiguration#OBJECT_BINARY_MAPPING} and the possible values are
 * {@link #SIMPLE},{@link #SINGLE_ORECORD_BYTES} or {@link #SPLIT_ORECORD_BYTES}.
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class OObjectFieldHandler {

  public static final int                           SIMPLE               = 0;
  public static final int                           SINGLE_ORECORD_BYTES = 1;
  public static final int                           SPLIT_ORECORD_BYTES  = 2;

  private static final OObjectFieldHandlingStrategy STRATEGY;
  static {
    Map<OType, OObjectFieldOTypeHandlingStrategy> typeHandlingStrategies = new HashMap<OType, OObjectFieldOTypeHandlingStrategy>();

    switch (OGlobalConfiguration.OBJECT_BINARY_MAPPING.getValueAsInteger()) {
    case SINGLE_ORECORD_BYTES:
      typeHandlingStrategies.put(OType.BINARY, new OObjectSingleRecordBytesOTypeHandlingStrategy());
      break;

    case SPLIT_ORECORD_BYTES:
      typeHandlingStrategies.put(OType.BINARY, new OObjectSplitRecordBytesOTypeHandlingStrategy());
      break;

    case SIMPLE:
    default:
      break;
    }

    STRATEGY = new OObjectSmartFieldHandlingStrategy(typeHandlingStrategies);
  }

  public static final OObjectFieldHandlingStrategy getStrategy() {
    return STRATEGY;
  }
}
