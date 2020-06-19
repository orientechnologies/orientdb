package org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideeffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

public final class OrientGraphMatchStepStrategy
    extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final OrientGraphMatchStepStrategy INSTANCE = new OrientGraphMatchStepStrategy();

  private OrientGraphMatchStepStrategy() {}

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {

    if (traversal.getSteps().size() >= 2) {
      final Step<?, ?> startStep = traversal.getStartStep();
      Step<?, ?> nextStep = startStep.getNextStep();
      if (startStep instanceof OrientGraphStep && nextStep instanceof MatchStep) {
        OrientGraphStep orientGraphStep = (OrientGraphStep) startStep;

        if (orientGraphStep.getHasContainers().size() == 0) {
          MatchStep matchStep = (MatchStep) nextStep;
          List<Traversal.Admin<Object, Object>> globalChildren = matchStep.getGlobalChildren();
          Traversal.Admin<Object, Object> match = globalChildren.iterator().next();
          Step<?, ?> currentStep = match.getStartStep().getNextStep();

          while (currentStep instanceof HasContainerHolder) {
            ((HasContainerHolder) currentStep)
                .getHasContainers()
                .forEach(orientGraphStep::addHasContainer);
            currentStep.getLabels().forEach(orientGraphStep::addLabel);
            match.removeStep(currentStep);
            currentStep = currentStep.getNextStep();
          }
        }
      }
    }
  }

  @Override
  public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
    return Collections.singleton(OrientGraphStepStrategy.class);
  }

  public static OrientGraphMatchStepStrategy instance() {
    return INSTANCE;
  }
}
