package com.tinkermic.gremlin;

import com.tinkermic.gremlin.structure.TinkermicGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(DatomicStrategySuite.class)
@GraphProviderClass(provider = TinkermicGraphProvider.class, graph = TinkermicGraph.class)
public class DatomicStrategyTest {
}
