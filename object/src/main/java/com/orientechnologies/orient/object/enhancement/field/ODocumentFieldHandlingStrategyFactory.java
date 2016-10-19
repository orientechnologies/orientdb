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

package com.orientechnologies.orient.object.enhancement.field;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Factory for {@link ODocumentFieldHandlingStrategy} instances.
 * <p/>
 * 
 * Since strategies are stateless, {@link ODocumentFieldHandlingStrategyRegistry} is used for caching.
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class ODocumentFieldHandlingStrategyFactory {

  public static final int                                    SIMPLE               = 0;
  public static final int                                    SINGLE_ORECORD_BYTES = 1;
  public static final int                                    SPLIT_ORECORD_BYTES  = 2;

  private static final ODocumentFieldHandlingStrategyFactory INSTANCE             = new ODocumentFieldHandlingStrategyFactory();

  private ODocumentFieldHandlingStrategyFactory() {
    // Hidden
  }

  /**
   * @return an instance
   */
  public static ODocumentFieldHandlingStrategyFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Creates a new instance of the requested strategy. Since strategies are stateless, if an existing instance already exists then
   * it's returned.
   * 
   * @param strategy
   * @return strategy instance
   */
  public ODocumentFieldHandlingStrategy create(int strategy) {

    Optional<ODocumentFieldHandlingStrategy> registered = ODocumentFieldHandlingStrategyRegistry.getInstance()
        .getStrategy(strategy);
    if (registered.isPresent()) {
      return registered.get();
    }

    Map<OType, ODocumentFieldOTypeHandlingStrategy> typeHandlingStrategies = new HashMap<OType, ODocumentFieldOTypeHandlingStrategy>();

    switch (strategy) {
    case SINGLE_ORECORD_BYTES:
      typeHandlingStrategies.put(OType.BINARY, new ODocumentSingleRecordBytesOTypeHandlingStrategy());
      break;

    case SPLIT_ORECORD_BYTES:
      typeHandlingStrategies.put(OType.BINARY, new ODocumentSplitRecordBytesOTypeHandlingStrategy());
      break;

    case SIMPLE:
    default:
      break;
    }

    ODocumentSmartFieldHandlingStrategy strategyInstance = new ODocumentSmartFieldHandlingStrategy(typeHandlingStrategies);
    ODocumentFieldHandlingStrategyRegistry.getInstance().registerStrategy(strategy, strategyInstance);
    return strategyInstance;
  }
}
