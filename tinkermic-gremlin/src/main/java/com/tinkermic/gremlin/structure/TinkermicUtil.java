package com.tinkermic.gremlin.structure;

import clojure.lang.Keyword;
import com.google.common.collect.ImmutableMap;
import datomic.Connection;
import datomic.Database;
import datomic.Peer;
import datomic.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class TinkermicUtil {
    private static final Map<String, String> types;

    static {
        // Types supported by the underlying Datomic data model
        types = ImmutableMap.<String, String>builder()
                .put("java.lang.String", ":db.type/string")
                .put("java.lang.Boolean", ":db.type/boolean")
                .put("java.lang.Long", ":db.type/long")
                .put("java.lang.Integer", ":db.type/long")
                .put("java.math.BigInteger", ":db.type/bigint")
                .put("java.lang.Float", ":db.type/float")
                .put("java.lang.Double", ":db.type/double")
                .put("java.math.BigDecimal", ":db.type/bigdec")
                .put("java.util.Date", ":db.type/instant")
                .put("java.util.UUID", ":db.type/uuid")
                .put("java.net.URI", ":db.type/uri")
                .build();
    }

    // Check whether a key is part of the reserved space
    public static boolean isReservedKey(String key) {
        // Key specific to the graph model or the general Datomic namespace
        return (key.startsWith(":graph") || key.startsWith(":db/"));
    }

    // Retrieve the original name of a property
    public static Optional<String> getPropertyName(String property) {
        if (property.equals("graph.edge/label")) return Optional.of("label");
        else if (property.equals("graph.vertex/label")) return Optional.of("label");
        else if (property.contains(".")) {
            return Optional.of(StringUtils.replaceChars(property.substring(1, property.indexOf(".")), "$", "_"));
        }
        return Optional.empty();
    }

    // Retrieve the Datomic to for the Java equivalent
    public static String javaTypeToDatomicType(Class clazz) {
        if (types.containsKey(clazz.getName())) {
            return types.get(clazz.getName());
        }
        throw new IllegalArgumentException("Object type " + clazz.getName() + " not supported");
    }

    // Checks whether a new attribute defintion needs to be created on the fly
    public static boolean attributeDefinitionExists(String key, Connection connection) {
        return !Peer.q("[:find ?a :in $ ?key :where [?a :db/ident ?key]]", connection.db(), key).isEmpty();
    }

    // Creates a unique key for each key-valuetype attribute (as only one attribute with the same name can be specified)
    public static String createKey(String key, Class<?> valueClazz, Class<?> elementClazz) {
        String elementType = elementClazz.isAssignableFrom(TinkermicEdge.class) ? "edge" : "vertex";
        return ":" + StringUtils.replaceChars(key, "_","$") + "." + javaTypeToDatomicType(valueClazz).split("/")[1] + "." + elementType;
    }

    // Helper method to create a mutable map (instead of an immutable map via the datomic Util.map method)
    public static Map map(Object... mapValues) {
        Map map = new HashMap();
        for (int i = 0; i < mapValues.length; i = i + 2) {
            map.put(mapValues[i], mapValues[i + 1]);
        }
        return map;
    }

    // Helper method to construct the difference (as a set of facts) between 2 sets of facts
    // The difference is calculated as a symmetric difference, while only maintaining the facts of the first set
    public static Set<Object> difference(Set<Object> facts1, Set<Object> facts2) {
        // Copy the set first
        Set<Object> difference = ((Set) ((HashSet) facts1).clone());
        Iterator<Object> facts1it = facts1.iterator();
        // Check which facts are exclusively part of the facts1 set
        while (facts1it.hasNext()) {
            Map fact = (Map) facts1it.next();
            if (!isGraphElementTypeFact(fact) && !isDbIdentFact(fact)) {
                // Check whether this fact is also available in facts2. If so, remove the element
                if (facts2.contains(fact)) {
                    difference.remove(fact);
                }
            }
        }
        // Return the normalized difference with newly generated temporary id
        normalize(difference);
        replaceWithTempId(difference);
        return difference;
    }

    // Helper method to normalize a set of facts (effectively removing facts (vertices or edges) that have no other attributes or are used as values of other facts
    public static void normalize(Set<Object> facts) {
        Iterator<Object> factsit = facts.iterator();
        while (factsit.hasNext()) {
            Map fact = (Map) factsit.next();
            // If it defines an element (vertex or edge)
            if (isGraphElementTypeFact(fact)) {
                // Get the id
                Object entityId = fact.get(":db/id");
                boolean found = false;
                // Check whether we find other facts (either as id or as value itself) that refer to this entity
                for (Object otherFact : facts) {
                    if (((Map) otherFact).containsValue(entityId) && !isGraphElementTypeFact((Map) otherFact) && !isDbIdentFact((Map) otherFact)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    factsit.remove();
                }
            }
        }
    }

    // Helper method to replace actual id's with temporary id's (use for creating the graph difference)
    public static void replaceWithTempId(Set<Object> facts) {
        Set<Object> originalIdFacts = new HashSet<Object>();
        for (Object fact : facts) {

            // Get the id of the entity
            Object id = ((Map) fact).get(":db/id");
            Object label = ((Map) fact).get(":graph.vertex/label");

            // If the id is still a long, it's need to be replace with a new datomic temporary id
            if (id instanceof Long) {
                // Create a temp id
                Object newId;
                // Add the existing id as a fact so that it can be retrieved as a property
                // Depending on the type of element, a different property name needs to be used in order to make it transparent at the graph level
                if (label != null) {
                    newId = Peer.tempid(":vertex");
                    originalIdFacts.add(Util.map(":db/id", newId, ":original$id.long.vertex", id));
                } else {
                    newId = Peer.tempid(":edge");
                    originalIdFacts.add(Util.map(":db/id", newId, ":original$id.long.edge", id));
                }
                ((Map) fact).put(":db/id", newId);

                // Replace all facts that have this id or use this id with the newly generate temp id
                for (Object otherfact : facts) {
                    Set<Object> keys = ((Map) otherfact).keySet();
                    for (Object key : keys) {
                        if (((Map) otherfact).get(key).equals(id)) {
                            ((Map) otherfact).put(key, newId);
                        }
                    }
                }
            }
        }
        // Add the original id facts
        facts.addAll(originalIdFacts);
    }

    private static boolean isGraphElementTypeFact(Map fact) {
        return fact.containsKey(":graph.element/type");
    }

    private static boolean isDbIdentFact(Map fact) {
        return fact.containsKey(":db/ident");
    }

    static UUID externalIdToUuid(Object id) throws IllegalArgumentException {
        if (null == id) {
            return null;
        } else if (id instanceof UUID) {
            return (UUID) id;
        } else if (id instanceof String) {
            return UUID.fromString(id.toString());
        } else if (id instanceof Vertex) {
            return (UUID) ((Vertex) id).id();
        } else if (id instanceof Edge) {
            return (UUID) ((Edge) id).id();
        } else {
            throw new IllegalArgumentException("Id cannot be interpreted as a graph UUID: " + id);
        }
    }
}
