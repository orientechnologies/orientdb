/*
 * Copyright 2010-2012 henryzhao81-at-gmail.com
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

package com.orientechnologies.orient.core.schedule;

import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import java.util.Date;
import java.util.Map;

/**
 * Builds a OSchedulerEvent with a fluent interface
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @since v2.2
 */
public class OScheduledEventBuilder extends ODocumentWrapper {
  public OScheduledEventBuilder() {
    super(new ODocument(OScheduledEvent.CLASS_NAME));
  }

  /** Creates a scheduled event object from a configuration. */
  public OScheduledEventBuilder(final ODocument doc) {
    super(doc);
  }

  public OScheduledEventBuilder setFunction(final OFunction function) {
    document.field(OScheduledEvent.PROP_FUNC, function);
    return this;
  }

  public OScheduledEventBuilder setRule(final String rule) {
    document.field(OScheduledEvent.PROP_RULE, rule);
    return this;
  }

  public OScheduledEventBuilder setArguments(final Map<Object, Object> arguments) {
    document.field(OScheduledEvent.PROP_ARGUMENTS, arguments);
    return this;
  }

  public OScheduledEventBuilder setStartTime(final Date startTime) {
    document.field(OScheduledEvent.PROP_STARTTIME, startTime);
    return this;
  }

  public OScheduledEvent build() {
    return new OScheduledEvent(document);
  }

  public String toString() {
    return document.toString();
  }

  public OScheduledEventBuilder setName(final String name) {
    document.field(OScheduledEvent.PROP_NAME, name);
    return this;
  }
}
