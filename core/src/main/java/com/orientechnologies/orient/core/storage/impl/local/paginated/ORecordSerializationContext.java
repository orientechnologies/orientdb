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

import com.orientechnologies.orient.core.OShutdownListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 11/26/13
 */
public class ORecordSerializationContext {

  private static volatile ThreadLocal<Deque<ORecordSerializationContext>> SERIALIZATION_CONTEXT_STACK = new ThreadLocal<Deque<ORecordSerializationContext>>() {
                                                                                                        @Override
                                                                                                        protected Deque<ORecordSerializationContext> initialValue() {
                                                                                                          return new ArrayDeque<ORecordSerializationContext>();
                                                                                                        }
                                                                                                      };

  static {
    Orient.instance().addShutdownListener(new OShutdownListener() {
      @Override
      public void onShutdown() {
        SERIALIZATION_CONTEXT_STACK = null;
      }
    });
  }

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

  private final Deque<ORecordSerializationOperation> operations = new ArrayDeque<ORecordSerializationOperation>();

  public void push(ORecordSerializationOperation operation) {
    operations.push(operation);
  }

  public void executeOperations(OAbstractPaginatedStorage storage) {
    for (ORecordSerializationOperation operation : operations) {
      operation.execute(storage);
    }
  }
}
