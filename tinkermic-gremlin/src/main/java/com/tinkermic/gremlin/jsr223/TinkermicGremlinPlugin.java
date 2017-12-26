package com.tinkermic.gremlin.jsr223;

import com.tinkermic.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

public final class TinkermicGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "tinkermic.imports";

    private static final ImportCustomizer imports;

    static {
        try {
            imports = DefaultImportCustomizer.build()
                    .addClassImports(TinkermicEdge.class,
                            TinkermicElement.class,
                            TinkermicGraph.class,
                            TinkermicHelper.class,
                            TinkermicProperty.class,
                            TinkermicVertex.class,
                            TinkermicVertexProperty.class)
                    .create();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final TinkermicGremlinPlugin instance = new TinkermicGremlinPlugin();

    public TinkermicGremlinPlugin() {
        super(NAME, imports);
    }

    public static TinkermicGremlinPlugin instance() {
        return instance;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}