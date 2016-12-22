package com.tinkermic.gremlin;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class DatomicStrategySuite extends AbstractGremlinSuite {
    public DatomicStrategySuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder,
                new Class<?>[]{
                        DatomicGraphStepStrategyTest.class
                }, new Class<?>[]{
                        DatomicGraphStepStrategyTest.class
                },
                false,
                TraversalEngine.Type.STANDARD);
    }
}