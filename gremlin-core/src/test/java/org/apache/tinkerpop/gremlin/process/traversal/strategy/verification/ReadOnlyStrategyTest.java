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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class ReadOnlyStrategyTest {
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"addE(test).from(x)", new DefaultGraphTraversal<>(EmptyGraph.instance()).addE("test").from("x")},
                {"addE(test).from(x).property(this,that)", new DefaultGraphTraversal<>(EmptyGraph.instance()).addE("test").from("x").property("this", "that")},
                {"addE(test).to(x)", new DefaultGraphTraversal<>(EmptyGraph.instance()).addE("test").to("x")},
                {"addE(test).to(x).property(this,that)", new DefaultGraphTraversal<>(EmptyGraph.instance()).addE("test").to("x").property("this", "that")},
                {"outE().property(k,v)", new DefaultGraphTraversal<>(EmptyGraph.instance()).outE().property("test", "test")},
                {"out().properties(k).property(k,v)", new DefaultGraphTraversal<>(EmptyGraph.instance()).out().properties("test").property("test", "that")},
                {"out().property(k,v)", new DefaultGraphTraversal<>(EmptyGraph.instance()).out().property("test", "test")},
                {"out().property(Cardinality,k,v)", new DefaultGraphTraversal<>(EmptyGraph.instance()).out().property(VertexProperty.Cardinality.list, "test", "test")},
                {"addV(person)", new DefaultGraphTraversal<>(EmptyGraph.instance()).addV("person")},
                {"addV()", new DefaultGraphTraversal<>(EmptyGraph.instance()).addV()}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Test
    public void shouldBeVerifiedIllegal() {
        try {
            ReadOnlyStrategy.instance().apply(this.traversal.asAdmin());
            fail("The strategy should have found a mutating step.");
        } catch (VerificationException ise) {
            assertTrue(ise.getMessage().startsWith("The provided traversal has a mutating step and thus is not read only: "));
        }
    }
}
