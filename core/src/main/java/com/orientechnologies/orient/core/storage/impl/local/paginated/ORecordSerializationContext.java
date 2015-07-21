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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 11/26/13
 */
public class ORecordSerializationContext {

  private static volatile ThreadLocal<Deque<ORecordSerializationContext>> SERIALIZATION_CONTEXT_STACK = new SerializationContextThreadLocal();

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (SERIALIZATION_CONTEXT_STACK == null)
          SERIALIZATION_CONTEXT_STACK = new SerializationContextThreadLocal();
      }

      @Override
      public void onShutdown() {
        SERIALIZATION_CONTEXT_STACK = null;
      }
    });
  }
  private final Deque<ORecordSerializationOperation>                      operations                  = new ArrayDeque<ORecordSerializationOperation>();

  public static int getDepth() {
    return ORecordSerializationContext.SERIALIZATION_CONTEXT_STACK.get().size();
  }

  public static ORecordSerializationContext pushContext() {
    final Deque<ORecordSerializationContext> stack = SERIALIZATION_CONTEXT_STACK.get();

    final ORecordSerializationContext context = new ORecordSerializationContext();
    stack.push(context);
    return context;
  }

  public static ORecordSerializationContext getContext() {
    final Deque<ORecordSerializationContext> stack = SERIALIZATION_CONTEXT_STACK.get();
    if (stack.isEmpty())
      return null;

    return stack.peek();
  }

  public static ORecordSerializationContext pullContext() {
    final Deque<ORecordSerializationContext> stack = SERIALIZATION_CONTEXT_STACK.get();
    if (stack.isEmpty())
      throw new IllegalStateException("Can not find current serialization context");

    return stack.poll();
  }

  public void push(ORecordSerializationOperation operation) {
    operations.push(operation);
  }

  public void executeOperations(OAbstractPaginatedStorage storage) {
    for (ORecordSerializationOperation operation : operations) {
      operation.execute(storage);
    }
  }

  private static class SerializationContextThreadLocal extends ThreadLocal<Deque<ORecordSerializationContext>> {
    @Override
    protected Deque<ORecordSerializationContext> initialValue() {
      return new ArrayDeque<ORecordSerializationContext>();
    }
  }
}
