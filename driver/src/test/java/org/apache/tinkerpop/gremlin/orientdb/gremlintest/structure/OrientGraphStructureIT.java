/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.orientdb.gremlintest.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.gremlintest.OrientGraphProvider;
import org.apache.tinkerpop.gremlin.structure.StructureIntegrateSuite;
import org.junit.runner.RunWith;

/**
 * Executes the Gremlin Structure Performance Test Suite using OrientGraph.
 *
 * <p>Extracted from TinkerGraph tests
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marvin Froeder (about.me/velo)
 */
@RunWith(StructureIntegrateSuite.class)
@GraphProviderClass(provider = OrientGraphProvider.class, graph = OrientGraph.class)
public class OrientGraphStructureIT {}
