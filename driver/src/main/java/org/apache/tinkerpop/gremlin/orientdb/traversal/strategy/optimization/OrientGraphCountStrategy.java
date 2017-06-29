package org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.orientdb.traversal.step.map.OrientClassCountStep;
import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideEffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This strategy will try to optimize the Count Step when it's possible.
 *
 *
 * @author Enrico Risa
 * @example <pre>
 * g.V().count()                               // is replaced by OrientClassCountStep
 * g.E().count()                               // is replaced by OrientClassCountStep
 * g.V().hasLabel('Foo').count()               // is replaced by OrientClassCountStep
 * g.E().hasLabel('Foo').count()               // is replaced by OrientClassCountStep
 *
 * </pre>
 */
public class OrientGraphCountStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final OrientGraphCountStrategy INSTANCE = new OrientGraphCountStrategy();

    private OrientGraphCountStrategy() {

    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {

        if (!(traversal.getParent() instanceof EmptyStep) || TraversalHelper.onGraphComputer(traversal))
            return;

        List<Step> steps = traversal.getSteps();
        if (steps.size() < 2)
            return;

        Step<?, ?> startStep = traversal.getStartStep();
        Step<?, ?> endStep = traversal.getEndStep();
        if (steps.size() == 2 && startStep instanceof OrientGraphStep && endStep instanceof CountGlobalStep) {

            OrientGraphStep step = (OrientGraphStep) startStep;
            if (step.getHasContainers().size() == 1) {
                Optional<String> className = step.getHasContainers().stream()
                        .filter(f -> ((HasContainer) f).getKey().equals("~label") && ((HasContainer) f).getBiPredicate() instanceof Compare)
                        .map(f -> ((HasContainer) f).getValue().toString()).findFirst();

                if (className.isPresent()) {
                    TraversalHelper.removeAllSteps(traversal);
                    traversal.addStep(new OrientClassCountStep(traversal, className.get()));
                }
            } else if (step.getHasContainers().size() == 0) {
                TraversalHelper.removeAllSteps(traversal);
                traversal.addStep(new OrientClassCountStep(traversal, step));
            }
        }

    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Collections.singleton(OrientGraphStepStrategy.class);
    }

    public static OrientGraphCountStrategy instance() {
        return INSTANCE;
    }
}
