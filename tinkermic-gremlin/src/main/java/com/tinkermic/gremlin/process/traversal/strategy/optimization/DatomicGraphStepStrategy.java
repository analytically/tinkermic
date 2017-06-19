package com.tinkermic.gremlin.process.traversal.strategy.optimization;

import com.tinkermic.gremlin.process.traversal.step.sideEffect.DatomicGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class DatomicGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final DatomicGraphStepStrategy INSTANCE = new DatomicGraphStepStrategy();

    private DatomicGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            final DatomicGraphStep<?, ?> datomicGraphStep = new DatomicGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, datomicGraphStep, traversal);
            Step<?, ?> currentStep = datomicGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(datomicGraphStep, hasContainer))
                            datomicGraphStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static DatomicGraphStepStrategy instance() {
        return INSTANCE;
    }
}