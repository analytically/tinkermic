package com.tinkermic.gremlin;

import com.tinkermic.gremlin.structure.TinkermicGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = TinkermicGraphProvider.class, graph = TinkermicGraph.class)
public class TinkermicStructureStandardTest {
}
