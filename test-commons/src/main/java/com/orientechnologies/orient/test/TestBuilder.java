/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author richter
 * @param <T> see {@link TestFactory}
 */
public class TestBuilder<T> {
  private final List<Callable<T>> workers = new ArrayList<Callable<T>>();

  public TestBuilder<T> add(int threadCount, TestFactory<T> factory) {
    workers.addAll(ConcurrentTestHelper.prepareWorkers(threadCount, factory));
    return this;
  }

  public Collection<T> go() {
    return ConcurrentTestHelper.go(workers);
  }
}
