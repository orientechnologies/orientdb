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

/**
 * {@link ODocumentFieldHandlingStrategy} registry.
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class ODocumentFieldHandlingStrategyRegistry {

  private static final ODocumentFieldHandlingStrategyRegistry INSTANCE   = new ODocumentFieldHandlingStrategyRegistry();

  private Map<Integer, ODocumentFieldHandlingStrategy>        strategies = new HashMap<Integer, ODocumentFieldHandlingStrategy>();

  private ODocumentFieldHandlingStrategyRegistry() {
    // Hidden
  }

  /**
   * @return an instance
   */
  public static ODocumentFieldHandlingStrategyRegistry getInstance() {
    return INSTANCE;
  }

  /**
   * Gets a registered strategy
   * 
   * @param strategy
   * @return registered strategy
   */
  public Optional<ODocumentFieldHandlingStrategy> getStrategy(int strategy) {
    return Optional.ofNullable(this.strategies.get(strategy));
  }

  /**
   * Registers a strategy
   * 
   * @param strategy
   * @param strategyInstance
   */
  public void registerStrategy(int strategy, ODocumentFieldHandlingStrategy strategyInstance) {
    this.strategies.put(strategy, strategyInstance);
  }
}
