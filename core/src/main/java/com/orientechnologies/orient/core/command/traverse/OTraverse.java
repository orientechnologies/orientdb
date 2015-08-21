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
package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.orient.core.command.OCommand;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Base class for traversing.
 * 
 * @author Luca Garulli
 */
public class OTraverse implements OCommand, Iterable<OIdentifiable>, Iterator<OIdentifiable> {
  private OCommandPredicate                 predicate;
  private Iterator<? extends OIdentifiable> target;
  private List<Object>                      fields      = new ArrayList<Object>();
  private long                              resultCount = 0;
  private long                              limit       = 0;
  private OIdentifiable                     lastTraversed;
  private STRATEGY                          strategy    = STRATEGY.DEPTH_FIRST;
  private OTraverseContext                  context     = new OTraverseContext();
  private int                               maxDepth    = -1;

  public enum STRATEGY {
    DEPTH_FIRST, BREADTH_FIRST
  }

  /*
   * Executes a traverse collecting all the result in the returning List<OIdentifiable>. This could be memory expensive because for
   * large results the list could be huge. it's always better to use it as an Iterable and lazy fetch each result on next() call.
   * 
   * @see com.orientechnologies.orient.core.command.OCommand#execute()
   */
  public List<OIdentifiable> execute() {
    final List<OIdentifiable> result = new ArrayList<OIdentifiable>();
    while (hasNext())
      result.add(next());
    return result;
  }

  public OTraverseAbstractProcess<?> nextProcess() {
    return context.next();
  }

  public boolean hasNext() {
    if (limit > 0 && resultCount >= limit)
      return false;

    if (lastTraversed == null)
      // GET THE NEXT
      lastTraversed = next();

    if (lastTraversed == null && !context.isEmpty())
      throw new IllegalStateException("Traverse ended abnormally");

    if (!OCommandExecutorAbstract.checkInterruption(context))
      return false;

    // BROWSE ALL THE RECORDS
    return lastTraversed != null;
  }

  public OIdentifiable next() {
    if (Thread.interrupted())
      throw new OCommandExecutionException("The traverse execution has been interrupted");

    if (lastTraversed != null) {
      // RETURN LATEST AND RESET IT
      final OIdentifiable result = lastTraversed;
      lastTraversed = null;
      return result;
    }

    if (limit > 0 && resultCount >= limit)
      return null;

    OIdentifiable result;
    OTraverseAbstractProcess<?> toProcess;
    // RESUME THE LAST PROCESS
    while ((toProcess = nextProcess()) != null) {
      result = toProcess.process();
      if (result != null) {
        resultCount++;
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

  public OTraverseContext getContext() {
    return context;
  }

  public OTraverse target(final Iterable<? extends OIdentifiable> iTarget) {
    return target(iTarget.iterator());
  }

  public OTraverse target(final OIdentifiable... iRecords) {
    final List<OIdentifiable> list = new ArrayList<OIdentifiable>();
    Collections.addAll(list, iRecords);
    return target(list.iterator());
  }

  @SuppressWarnings("unchecked")
  public OTraverse target(final Iterator<? extends OIdentifiable> iTarget) {
    target = iTarget;
    context.reset();
    new OTraverseRecordSetProcess(this, (Iterator<OIdentifiable>) target, OTraversePath.empty());
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

  public OTraverse field(final Object iField) {
    if (!fields.contains(iField))
      fields.add(iField);
    return this;
  }

  public OTraverse fields(final Collection<Object> iFields) {
    for (Object f : iFields)
      field(f);
    return this;
  }

  public OTraverse fields(final String... iFields) {
    for (String f : iFields)
      field(f);
    return this;
  }

  public List<Object> getFields() {
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

  @Override
  public String toString() {
    return String.format("OTraverse.target(%s).fields(%s).limit(%d).predicate(%s)", target, fields, limit, predicate);
  }

  public long getResultCount() {
    return resultCount;
  }

  public OIdentifiable getLastTraversed() {
    return lastTraversed;
  }

  public STRATEGY getStrategy() {
    return strategy;
  }

  public void setStrategy(STRATEGY strategy) {
    this.strategy = strategy;
    context.setStrategy(strategy);
  }

  public int getMaxDepth() {
    return maxDepth;
  }

  public void setMaxDepth(final int maxDepth) {
    this.maxDepth = maxDepth;
  }
}
