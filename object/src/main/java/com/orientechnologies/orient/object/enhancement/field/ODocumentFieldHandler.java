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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;

/**
 * Wraps a {@link ODocumentFieldHandlingStrategy} to be accessed statically.<br/>
 * The strategy to be used is set via {@link OGlobalConfiguration#DOCUMENT_BINARY_MAPPING} and the possible values are
 * {@link ODocumentFieldHandlingStrategyFactory#SIMPLE}, {@link ODocumentFieldHandlingStrategyFactory#SINGLE_ORECORD_BYTES} or
 * {@link ODocumentFieldHandlingStrategyFactory#SPLIT_ORECORD_BYTES}.
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class ODocumentFieldHandler {

  private ODocumentFieldHandler() {
    // hidden
  }

  public static final ODocumentFieldHandlingStrategy getStrategy(ODatabase<?> database) {
    int strategy = database.getConfiguration().getValueAsInteger(OGlobalConfiguration.DOCUMENT_BINARY_MAPPING);
    return ODocumentFieldHandlingStrategyFactory.getInstance().create(strategy);
  }
}
