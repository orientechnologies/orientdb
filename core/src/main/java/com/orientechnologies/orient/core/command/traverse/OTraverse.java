/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.command.traverse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommand;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Base class for traversing.
 * 
 * @author Luca Garulli
 */
public class OTraverse implements OCommand, Iterable<OIdentifiable>, Iterator<OIdentifiable> {
  private OTraverseContext                  context  = new OTraverseContext();
  private OCommandPredicate                 predicate;
  private Iterator<? extends OIdentifiable> target;
  private List<String>                      fields   = new ArrayList<String>();
  private long                              returned = 0;
  private long                              limit    = 0;

  private OIdentifiable                     lastTraversed;

  /*
   * Executes a traverse collecting all the result in the returning List<OIdentifiable>. This could be memory expensive because for
   * large results the list could be huge. it's always better to use it as an Iterable and lazy fetch each result on next() call.
   * 
   * @see com.orientechnologies.orient.core.command.OCommand#execute()
   */
  public Object execute() {
    final List<OIdentifiable> result = new ArrayList<OIdentifiable>();
    while (hasNext())
      result.add(next());
    return result;
  }

  public boolean hasNext() {
    if (limit > 0 && returned >= limit)
      return false;

    if (lastTraversed == null)
      // GET THE NEXT
      lastTraversed = next();

    if (lastTraversed == null && context.peek() != null)
      throw new IllegalStateException("Traverse ended abnormally");

    // BROWSE ALL THE RECORDS
    return lastTraversed != null;
  }

  public OIdentifiable next() {
    if (lastTraversed != null) {
      // RETURN LATEST AND RESET IT
      final OIdentifiable result = lastTraversed;
      lastTraversed = null;
      return result;
    }

    if (limit > 0 && returned >= limit)
      return null;

    OIdentifiable result;
    OTraverseAbstractProcess<?> toProcess;
    // RESUME THE LAST PROCESS
    while ((toProcess = (OTraverseAbstractProcess<?>) context.peek()) != null) {
      result = (OIdentifiable) toProcess.process();
      if (result != null) {
        returned++;
        return result;
      }
    }

    return null;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove()");
  }

  public Iterator<OIdentifiable> iterator() {
    return this;
  }

  public OTraverse context(final OCommandContext iContext) {
    if (iContext != null)
      iContext.merge(context);
    return this;
  }

  public OTraverseContext getContext() {
    return context;
  }

  @SuppressWarnings("unchecked")
  public OTraverse target(final Iterator<? extends OIdentifiable> iTarget) {
    target = iTarget;
    context.reset();
    new OTraverseRecordSetProcess(this, (Iterator<OIdentifiable>) target);
    return this;
  }

  public Iterator<? extends OIdentifiable> getTarget() {
    return target;
  }

  public OTraverse predicate(final OCommandPredicate iPredicate) {
    predicate = iPredicate;
    return this;
  }

  public OCommandPredicate getPredicate() {
    return predicate;
  }

  public OTraverse field(final String iFieldName) {
    if (!fields.contains(iFieldName))
      fields.add(iFieldName);
    return this;
  }

  public OTraverse fields(final Collection<String> iFields) {
    for (String f : iFields)
      field(f);
    return this;
  }

  public List<String> getFields() {
    return fields;
  }

  public long getLimit() {
    return limit;
  }

  public OTraverse limit(final long iLimit) {
    if (iLimit < -1)
      throw new IllegalArgumentException("Limit cannot be negative. 0 = infinite");
    this.limit = iLimit;
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return String.format("OTraverse.target(%s).fields(%s).limit(%d).predicate(%s)", target, fields, limit, predicate);
  }
}
