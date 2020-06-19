package org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideeffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class OrientGraphStepStrategy
    extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final OrientGraphStepStrategy INSTANCE = new OrientGraphStepStrategy();

  private OrientGraphStepStrategy() {}

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {
    Step current = traversal.getStartStep();
    do {
      current = replaceStrategy(traversal, current).getNextStep();
    } while (current != null && !(current instanceof EmptyStep));
  }

  private Step replaceStrategy(Traversal.Admin<?, ?> traversal, Step<?, ?> step) {
    if (step instanceof GraphStep && !(step instanceof OrientGraphStep)) {
      final GraphStep<?, ?> originalGraphStep = (GraphStep) step;
      final OrientGraphStep<?, ?> orientGraphStep = new OrientGraphStep<>(originalGraphStep);
      TraversalHelper.replaceStep(step, (Step) orientGraphStep, traversal);

      Step<?, ?> currentStep = orientGraphStep.getNextStep();
      while (currentStep instanceof HasContainerHolder) {
        ((HasContainerHolder) currentStep)
            .getHasContainers()
            .forEach(orientGraphStep::addHasContainer);
        currentStep.getLabels().forEach(orientGraphStep::addLabel);
        traversal.removeStep(currentStep);
        currentStep = currentStep.getNextStep();
      }
      return orientGraphStep;
    } else {
      return step;
    }
  }

  public static OrientGraphStepStrategy instance() {
    return INSTANCE;
  }
}
