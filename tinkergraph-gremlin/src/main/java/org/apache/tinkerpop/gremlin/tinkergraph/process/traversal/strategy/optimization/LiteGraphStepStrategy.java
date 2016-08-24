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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LiteGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final LiteGraphStepStrategy INSTANCE = new LiteGraphStepStrategy();

    private LiteGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
/*        if (TraversalHelper.onGraphComputer(traversal))
            return;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            final LiteGraphStep<?, ?> liteGraphStep = new LiteGraphStep<Object, Element>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, (Step) liteGraphStep, traversal);
            Step<?, ?> currentStep = liteGraphStep.getNextStep();
            while (currentStep instanceof HasContainerHolder) {
                ((HasContainerHolder) currentStep).getHasContainers().forEach(hasContainer -> {
                    if (!GraphStep.processHasContainerIds(liteGraphStep, hasContainer))
                        liteGraphStep.addHasContainer(hasContainer);
                });
                currentStep.getLabels().forEach(liteGraphStep::addLabel);
                traversal.removeStep(currentStep);
                currentStep = currentStep.getNextStep();
            }
        });*/
    }

    public static LiteGraphStepStrategy instance() {
        return INSTANCE;
    }
}
