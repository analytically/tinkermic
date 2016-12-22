package com.tinkermic.gremlin.process.traversal.step.sideEffect;

import com.tinkermic.gremlin.structure.TinkermicEdge;
import com.tinkermic.gremlin.structure.TinkermicGraph;
import com.tinkermic.gremlin.structure.TinkermicVertex;
import datomic.Database;
import datomic.Peer;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.StreamSupport;

public class DatomicGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {
    private final List<HasContainer> hasContainers = new ArrayList<>();

    public DatomicGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        TinkermicGraph graph = (TinkermicGraph) this.getTraversal().getGraph().get();

        // ids are present, filter on them first
        if (ids != null && ids.length > 0)
            return IteratorUtils.filter(graph.edges(ids), vertex -> HasContainer.testAll(vertex, hasContainers));

        // get a label being search on
        Optional<String> eqLabel = hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
                .map(hasContainer -> (String) hasContainer.getValue())
                .findAny();

        if (eqLabel.isPresent()) {
            graph.tx().readWrite();
            Database database = graph.database();

            // find an edge by label
            return StreamSupport.stream(listEdges(eqLabel.get(), database).spliterator(), false)
                    .map(v -> new TinkermicEdge(graph, Optional.of(database), (UUID) v.get(1), v.get(0), (String) v.get(2)))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers))
                    .iterator();
        }

        // linear scan
        return IteratorUtils.filter(graph.edges(), edge -> HasContainer.testAll(edge, this.hasContainers));
    }

    private Iterator<? extends Vertex> vertices() {
        TinkermicGraph graph = (TinkermicGraph) this.getTraversal().getGraph().get();

        // ids are present, filter on them first
        if (ids != null && ids.length > 0)
            return IteratorUtils.filter(graph.vertices(ids), vertex -> HasContainer.testAll(vertex, hasContainers));

        // get a label being search on
        Optional<String> eqLabel = hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
                .map(hasContainer -> (String) hasContainer.getValue())
                .findAny();

        if (eqLabel.isPresent()) {
            graph.tx().readWrite();
            Database database = graph.database();

            // find a vertex by label
            return listVertices(eqLabel.get(), database).stream()
                    .map(v -> new TinkermicVertex(graph, Optional.of(database), (UUID) v.get(1), v.get(0), (String) v.get(2)))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers))
                    .iterator();
        }

        // linear scan
        return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, hasContainers));
    }

    private Collection<List<Object>> listEdges(String label, Database database) {
        return Peer.q("[:find ?v ?uuid ?label :in $ ?label :where " +
                        "[?v :graph.element/id ?uuid] " +
                        "[?v :graph.edge/label ?label]]",
                database, label);
    }

    private Collection<List<Object>> listVertices(String label, Database database) {
        return Peer.q("[:find ?v ?uuid ?label :in $ ?label :where " +
                        "[?v :graph.element/id ?uuid] " +
                        "[?v :graph.vertex/label ?label]]",
                database, label);
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
