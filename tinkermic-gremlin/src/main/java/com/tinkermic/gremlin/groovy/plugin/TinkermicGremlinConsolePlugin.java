package com.tinkermic.gremlin.groovy.plugin;

import com.google.common.collect.ImmutableSet;
import com.tinkermic.gremlin.structure.TinkermicGraph;
import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;

import java.util.Set;

/**
 * Gremlin plugin for TinkerMic that provides a list of classes to import to the Gremlin Console.
 */
public final class TinkermicGremlinConsolePlugin extends AbstractGremlinPlugin {
    private static final Set<String> IMPORTS = ImmutableSet.of(
        IMPORT_SPACE + TinkermicGraph.class.getPackage().getName() + DOT_STAR
    );

    @Override
    public String getName() {
        return "tinkermic";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}