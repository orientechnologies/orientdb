/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.processor.block;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OIterateBlock extends OAbstractBlock {
  @Override
  public Object processBlock(final OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {

    Iterable<Object> result = null;

    final String var = getFieldOfClass(iContext, iConfig, "variable", String.class);
    final String range = getFieldOfClass(iContext, iConfig, "range", String.class);

    if (range != null) {
      final String[] fromTo = range.split("-");
      final int from = Integer.parseInt(fromTo[0]);
      final int to = Integer.parseInt(fromTo[1]);

      final Integer[] values = new Integer[Math.abs(to - from + 1)];
      if (from < to)
        for (int i = from; i <= to; ++i) {
          values[i - from] = i;
        }
      else
        for (int i = to; i >= from; --i) {
          values[to - i] = i;
        }

      result = new OIterateBlockIterable(values, iContext, var);
    }

    return result;
  }

  @Override
  public String getName() {
    return "iterate";
  }

  protected class OIterateBlockIterable implements Iterable<Object> {

    private final Object[]        objects;
    private final OCommandContext context;
    private final String          variableName;

    public OIterateBlockIterable(final Object[] o, final OCommandContext iContext, final String iVariableName) {
      objects = o;
      context = iContext;
      variableName = iVariableName;
    }

    public Iterator<Object> iterator() {
      return new OIterateBlockIterator();
    }

    private class OIterateBlockIterator implements Iterator<Object> {
      private int p = 0;

      public boolean hasNext() {
        return p < objects.length;
      }

      public Object next() {
        if (p < objects.length) {
          final Object value = objects[p++];

          context.setVariable(variableName, value);

          return value;
        } else {
          throw new NoSuchElementException();
        }
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    }
  }
}