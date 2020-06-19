package org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.orientdb.traversal.step.map.OrientClassCountStep;
import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideeffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * This strategy will try to optimize the Count Step when it's possible.
 *
 * @author Enrico Risa
 * @example
 *     <pre>
 * g.V().count()                               // is replaced by OrientClassCountStep
 * g.E().count()                               // is replaced by OrientClassCountStep
 * g.V().hasLabel('Foo').count()               // is replaced by OrientClassCountStep
 * g.E().hasLabel('Foo').count()               // is replaced by OrientClassCountStep
 * <p>
 * </pre>
 */
public class OrientGraphCountStrategy
    extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final OrientGraphCountStrategy INSTANCE = new OrientGraphCountStrategy();

  private OrientGraphCountStrategy() {}

  @Override
  public void apply(Traversal.Admin<?, ?> traversal) {

    if (!(traversal.getParent() instanceof EmptyStep) || TraversalHelper.onGraphComputer(traversal))
      return;

    List<Step> steps = traversal.getSteps();
    if (steps.size() < 2) return;

    Step<?, ?> startStep = traversal.getStartStep();
    Step<?, ?> endStep = traversal.getEndStep();
    if (steps.size() == 2
        && startStep instanceof OrientGraphStep
        && endStep instanceof CountGlobalStep) {

      OrientGraphStep step = (OrientGraphStep) startStep;

      if (step.getHasContainers().size() == 1) {
        List<HasContainer> hasContainers = step.getHasContainers();
        List<String> classes =
            hasContainers.stream()
                .filter(this::isLabelFilter)
                .map(this::extractLabels)
                .flatMap((s) -> s.stream())
                .collect(Collectors.toList());
        if (classes.size() > 0) {
          TraversalHelper.removeAllSteps(traversal);
          traversal.addStep(new OrientClassCountStep(traversal, classes, step.isVertexStep()));
        }
      } else if (step.getHasContainers().size() == 0) {
        TraversalHelper.removeAllSteps(traversal);
        String baseClass = step.isVertexStep() ? "V" : "E";
        traversal.addStep(
            new OrientClassCountStep(
                traversal, Collections.singletonList(baseClass), step.isVertexStep()));
      }
    }
  }

  protected boolean isLabelFilter(HasContainer f) {
    boolean labelFilter = f.getKey().equals("~label");

    BiPredicate<?, ?> predicate = f.getBiPredicate();

    if (predicate instanceof Compare) {
      return labelFilter && Compare.eq.equals(predicate);
    }
    if (predicate instanceof Contains) {
      return labelFilter && Contains.within.equals(predicate);
    }

    return false;
  }

  protected List<String> extractLabels(HasContainer f) {
    Object value = f.getValue();
    List<String> classLabels = new ArrayList<>();
    if (value instanceof List) {
      ((List) value).forEach(label -> classLabels.add((String) label));
    } else {
      classLabels.add((String) value);
    }
    return classLabels;
  }

  @Override
  public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
    return Collections.singleton(OrientGraphStepStrategy.class);
  }

  public static OrientGraphCountStrategy instance() {
    return INSTANCE;
  }
}
