/*****************************************************************************
 * Copyright (C) Compart AG, 2016 - Compart confidential
 *
 *****************************************************************************/

package com.orientechnologies.orient.object.enhancement.field;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * {@link ODocumentFieldHandlingStrategy} registry.
 * 
 * @author dta
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
