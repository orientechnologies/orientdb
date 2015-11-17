package org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideEffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class OrientGraphStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.VendorOptimizationStrategy>
        implements TraversalStrategy.VendorOptimizationStrategy {

    private static final OrientGraphStepStrategy INSTANCE = new OrientGraphStepStrategy();

    private OrientGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isComputer())
            return;

        final Step<?, ?> startStep = traversal.getStartStep();
        // only apply once
        if (startStep instanceof GraphStep && !(startStep instanceof OrientGraphStep)) {
            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
            final OrientGraphStep<?> orientGraphStep = new OrientGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(startStep, (Step) orientGraphStep, traversal);

            Step<?, ?> currentStep = orientGraphStep.getNextStep();
            while (currentStep instanceof HasContainerHolder) {
                ((HasContainerHolder) currentStep).getHasContainers().forEach(orientGraphStep::addHasContainer);
                currentStep.getLabels().forEach(orientGraphStep::addLabel);
                traversal.removeStep(currentStep);
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static OrientGraphStepStrategy instance() {
        return INSTANCE;
    }
}
