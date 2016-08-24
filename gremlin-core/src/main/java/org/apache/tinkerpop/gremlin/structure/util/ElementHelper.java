/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utility class supporting common functions for {@link org.apache.tinkerpop.gremlin.structure.Element}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ElementHelper {

    private ElementHelper() {
    }

    /**
     * Determine whether the {@link Element} label can be legally set. This is typically used as a pre-condition check.
     *
     * @param label the element label
     * @throws IllegalArgumentException whether the label is legal and if not, a clear reason exception is provided
     */
    public static void validateLabel(final String label) throws IllegalArgumentException {
        if (null == label)
            throw Element.Exceptions.labelCanNotBeNull();
        if (label.isEmpty())
            throw Element.Exceptions.labelCanNotBeEmpty();
        if (Graph.Hidden.isHidden(label))
            throw Element.Exceptions.labelCanNotBeAHiddenKey(label);
    }

    /**
     * Determine whether an array of ids are either all elements or ids of elements. This is typically used as a pre-condition check.
     *
     * @param clazz the class of the element for which the ids will bind
     * @param ids   the ids that must be either elements or id objects, else
     *              {@link org.apache.tinkerpop.gremlin.structure.Graph.Exceptions#idArgsMustBeEitherIdOrElement()} is thrown.
     */
    public static void validateMixedElementIds(final Class<? extends Element> clazz, final Object... ids) throws IllegalArgumentException {
        if (ids.length > 1) {
            final boolean element = clazz.isAssignableFrom(ids[0].getClass());
            for (int i = 1; i < ids.length; i++) {
                if (clazz.isAssignableFrom(ids[i].getClass()) != element)
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
        }
    }

    /**
     * Determines whether the property key/value for the specified thing can be legally set. This is typically used as
     * a pre-condition check prior to setting a property.
     *
     * @param key   the key of the property
     * @param value the value of the property
     * @throws IllegalArgumentException whether the key/value pair is legal and if not, a clear reason exception
     *                                  message is provided
     */
    public static void validateProperty(final String key, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Property.Exceptions.propertyValueCanNotBeNull();
        if (null == key)
            throw Property.Exceptions.propertyKeyCanNotBeNull();
        if (key.isEmpty())
            throw Property.Exceptions.propertyKeyCanNotBeEmpty();
        if (Graph.Hidden.isHidden(key))
            throw Property.Exceptions.propertyKeyCanNotBeAHiddenKey(key);
    }

    /**
     * Determines whether a list of key/values are legal, ensuring that there are an even number of values submitted
     * and that the key values in the list of arguments are {@link String} or {@link org.apache.tinkerpop.gremlin.structure.Element} objects.
     *
     * @param propertyKeyValues a list of key/value pairs
     * @throws IllegalArgumentException if something in the pairs is illegal
     */
    public static void legalPropertyKeyValueArray(final Object... propertyKeyValues) throws IllegalArgumentException {
        if (propertyKeyValues.length % 2 != 0)
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!(propertyKeyValues[i] instanceof String) && !(propertyKeyValues[i] instanceof T))
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
        }
    }

    /**
     * Extracts the value of the {@link T#id} key from the list of arguments.
     *
     * @param keyValues a list of key/value pairs
     * @return the value associated with {@link T#id}
     * @throws NullPointerException if the value for the {@link T#id} key is {@code null}
     */
    public static Optional<Object> getIdValue(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.id))
                return Optional.of(keyValues[i + 1]);
        }
        return Optional.empty();
    }

    /**
     * Remove a key from the set of key/value pairs. Assumes that validations have already taken place to
     * assure that key positions contain strings and that there are an even number of elements. If after removal
     * there are no values left, the key value list is returned as empty.
     *
     * @param keyToRemove the key to remove
     * @param keyValues   the list to remove the accessor from
     * @return the key/values without the specified accessor or an empty array if no values remain after removal
     */
    public static Optional<Object[]> remove(final String keyToRemove, final Object... keyValues) {
        return ElementHelper.remove((Object) keyToRemove, keyValues);
    }

    /**
     * Removes an accessor from the set of key/value pairs. Assumes that validations have already taken place to
     * assure that key positions contain strings and that there are an even number of elements. If after removal
     * there are no values left, the key value list is returned as empty.
     *
     * @param accessor  to remove
     * @param keyValues the list to remove the accessor from
     * @return the key/values without the specified accessor or an empty array if no values remain after removal
     */
    public static Optional<Object[]> remove(final T accessor, final Object... keyValues) {
        return ElementHelper.remove((Object) accessor, keyValues);
    }

    private static Optional<Object[]> remove(final Object keyToRemove, final Object... keyValues) {
        final List list = Arrays.asList(keyValues);
        final List revised = IntStream.range(0, list.size())
                .filter(i -> i % 2 == 0)
                .filter(i -> !keyToRemove.equals(list.get(i)))
                .flatMap(i -> IntStream.of(i, i + 1))
                .mapToObj(i -> list.get(i))
                .collect(Collectors.toList());
        return revised.size() > 0 ? Optional.of(revised.toArray()) : Optional.empty();
    }

    /**
     * Append a key/value pair to a list of existing key/values. If the key already exists in the keyValues then
     * that value is overwritten with the provided value.
     */
    public static Object[] upsert(final Object[] keyValues, final Object key, final Object val) {
        if (!getKeys(keyValues).contains(key))
            return Stream.concat(Stream.of(keyValues), Stream.of(key, val)).toArray();
        else {
            final Object[] kvs = new Object[keyValues.length];
            for (int i = 0; i < keyValues.length; i = i + 2) {
                kvs[i] = keyValues[i];
                if (keyValues[i].equals(key))
                    kvs[i + 1] = val;
                else
                    kvs[i + 1] = keyValues[i + 1];
            }

            return kvs;
        }
    }

    /**
     * Replaces one key with a different key.
     *
     * @param keyValues the list of key/values to alter
     * @param oldKey    the key to replace
     * @param newKey    the new key
     */
    public static Object[] replaceKey(final Object[] keyValues, final Object oldKey, final Object newKey) {
        final Object[] kvs = new Object[keyValues.length];
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(oldKey))
                kvs[i] = newKey;
            else
                kvs[i] = keyValues[i];
            kvs[i + 1] = keyValues[i + 1];
        }

        return kvs;
    }

    /**
     * Converts a set of key values to a Map.  Assumes that validations have already taken place to
     * assure that key positions contain strings and that there are an even number of elements.
     */
    public static Map<String, Object> asMap(final Object... keyValues) {
        return asPairs(keyValues).stream().collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));
    }

    /**
     * Convert a set of key values to a list of Pair objects.  Assumes that validations have already taken place to
     * assure that key positions contain strings and that there are an even number of elements.
     */
    public static List<Pair<String, Object>> asPairs(final Object... keyValues) {
        final List list = Arrays.asList(keyValues);
        return IntStream.range(1, list.size())
                .filter(i -> i % 2 != 0)
                .mapToObj(i -> Pair.with(list.get(i - 1).toString(), list.get(i)))
                .collect(Collectors.toList());
    }

    /**
     * Gets the list of keys from the key values.
     *
     * @param keyValues a list of key/values pairs
     */
    public static Set<String> getKeys(final Object... keyValues) {
        final Set<String> keys = new HashSet<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            keys.add(keyValues[i].toString());
        }
        return keys;
    }

    /**
     * Extracts the value of the {@link T#label} key from the list of arguments.
     *
     * @param keyValues a list of key/value pairs
     * @return the value associated with {@link T#label}
     * @throws ClassCastException   if the value of the label is not a {@link String}
     * @throws NullPointerException if the value for the {@link T#label} key is {@code null}
     */
    public static Optional<String> getLabelValue(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.label)) {
                ElementHelper.validateLabel((String) keyValues[i + 1]);
                return Optional.of((String) keyValues[i + 1]);
            }
        }
        return Optional.empty();
    }

    /**
     * Assign key/value pairs as properties to an {@link org.apache.tinkerpop.gremlin.structure.Element}.  If the value of {@link T#id} or
     * {@link T#label} is in the set of pairs, then they are ignored.
     *
     * @param element           the graph element to assign the {@code propertyKeyValues}
     * @param propertyKeyValues the key/value pairs to assign to the {@code element}
     * @throws ClassCastException       if the value of the key is not a {@link String}
     * @throws IllegalArgumentException if the value of {@code element} is null
     */
    public static void attachProperties(final Element element, final Object... propertyKeyValues) {
        if (null == element)
            throw Graph.Exceptions.argumentCanNotBeNull("element");

        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label))
                element.property((String) propertyKeyValues[i], propertyKeyValues[i + 1]);
        }
    }

    /**
     * Assign key/value pairs as properties to an {@link org.apache.tinkerpop.gremlin.structure.Vertex}.  If the value of {@link T#id} or
     * {@link T#label} is in the set of pairs, then they are ignored.
     * The {@link org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality} of the key is determined from the {@link org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures}.
     *
     * @param vertex            the graph vertex to assign the {@code propertyKeyValues}
     * @param propertyKeyValues the key/value pairs to assign to the {@code element}
     * @throws ClassCastException       if the value of the key is not a {@link String}
     * @throws IllegalArgumentException if the value of {@code element} is null
     */
    public static void attachProperties(final Vertex vertex, final Object... propertyKeyValues) {
        if (null == vertex)
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");

        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label))
                vertex.property(vertex.graph().features().vertex().getCardinality((String) propertyKeyValues[i]), (String) propertyKeyValues[i], propertyKeyValues[i + 1]);
        }
    }

    /**
     * Assign key/value pairs as properties to a {@link org.apache.tinkerpop.gremlin.structure.Vertex}.
     * If the value of {@link T#id} or {@link T#label} is in the set of pairs, then they are ignored.
     *
     * @param vertex            the vertex to attach the properties to
     * @param cardinality       the cardinality of the key value pair settings
     * @param propertyKeyValues the key/value pairs to assign to the {@code element}
     * @throws ClassCastException       if the value of the key is not a {@link String}
     * @throws IllegalArgumentException if the value of {@code element} is null
     */
    public static void attachProperties(final Vertex vertex, final VertexProperty.Cardinality cardinality, final Object... propertyKeyValues) {

        if (null == vertex) {
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        }
        //System.out.println("property number="+propertyKeyValues.length);
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label)) {
                //System.out.println("cardinality="+cardinality+"\tkey="+propertyKeyValues[i]+"\tvalue="+propertyKeyValues[i+1]);
                vertex.property(cardinality, (String) propertyKeyValues[i], propertyKeyValues[i + 1]);
            }
        }
    }

    /**
     * This is a helper method for dealing with vertex property cardinality and typically used in {@link Vertex#property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality, String, Object, Object...)}.
     * If the cardinality is list, simply return {@link Optional#empty}.
     * If the cardinality is single, delete all existing properties of the provided key and return {@link Optional#empty}.
     * If the cardinality is set, find one that has the same key/value and attached the properties to it and return it. Else, if no equal value is found, return {@link Optional#empty}.
     *
     * @param vertex      the vertex to stage a vertex property for
     * @param cardinality the cardinality of the vertex property
     * @param key         the key of the vertex property
     * @param value       the value of the vertex property
     * @param keyValues   the properties of vertex property
     * @param <V>         the type of the vertex property value
     * @return a vertex property if it has been found in set with equal value
     */
    public static <V> Optional<VertexProperty<V>> stageVertexProperty(final Vertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (cardinality.equals(VertexProperty.Cardinality.single))
            vertex.properties(key).forEachRemaining(VertexProperty::remove);
        else if (cardinality.equals(VertexProperty.Cardinality.set)) {
            final Iterator<VertexProperty<V>> itty = vertex.properties(key);
            while (itty.hasNext()) {
                final VertexProperty<V> property = itty.next();
                if (property.value().equals(value)) {
                    ElementHelper.attachProperties(property, keyValues);
                    return Optional.of(property);
                }
            }
        } // do nothing on Cardinality.list
        return Optional.empty();
    }

    /**
     * Retrieve the properties associated with a particular element.
     * The result is a Object[] where odd indices are String keys and even indices are the values.
     *
     * @param element          the element to retrieve properties from
     * @param includeId        include Element.ID in the key/value list
     * @param includeLabel     include Element.LABEL in the key/value list
     * @param propertiesToCopy the properties to include with an empty list meaning copy all properties
     * @return a key/value array of properties where odd indices are String keys and even indices are the values.
     */
    public static Object[] getProperties(final Element element, final boolean includeId, final boolean includeLabel, final Set<String> propertiesToCopy) {
        final List<Object> keyValues = new ArrayList<>();
        if (includeId) {
            keyValues.add(T.id);
            keyValues.add(element.id());
        }
        if (includeLabel) {
            keyValues.add(T.label);
            keyValues.add(element.label());
        }
        element.keys().forEach(key -> {
            if (propertiesToCopy.isEmpty() || propertiesToCopy.contains(key)) {
                keyValues.add(key);
                keyValues.add(element.value(key));
            }
        });
        return keyValues.toArray(new Object[keyValues.size()]);
    }

    /**
     * A standard method for determining if two {@link org.apache.tinkerpop.gremlin.structure.Element} objects are equal. This method should be used by any
     * {@link Object#equals(Object)} implementation to ensure consistent behavior. This method is used for Vertex, Edge, and VertexProperty.
     *
     * @param a The first {@link org.apache.tinkerpop.gremlin.structure.Element}
     * @param b The second {@link org.apache.tinkerpop.gremlin.structure.Element} (as an {@link Object})
     * @return true if elements and equal and false otherwise
     */
    public static boolean areEqual(final Element a, final Object b) {
        if (null == b || null == a)
            return false;

        if (a == b)
            return true;
        if (!((a instanceof Vertex && b instanceof Vertex) ||
                (a instanceof Edge && b instanceof Edge) ||
                (a instanceof VertexProperty && b instanceof VertexProperty)))
            return false;
        return haveEqualIds(a, (Element) b);
    }

    public static boolean areEqual(final Vertex a, final Vertex b) {
        return null != b && null != a && (a == b || haveEqualIds(a, b));
    }

    public static boolean areEqual(final Edge a, final Edge b) {
        return null != b && null != a && (a == b || haveEqualIds(a, b));
    }

    public static boolean areEqual(final VertexProperty a, final VertexProperty b) {
        return null != b && null != a && (a == b || haveEqualIds(a, b));
    }

    /**
     * A standard method for determining if two {@link org.apache.tinkerpop.gremlin.structure.VertexProperty} objects are equal. This method should be used by any
     * {@link Object#equals(Object)} implementation to ensure consistent behavior.
     *
     * @param a the first {@link org.apache.tinkerpop.gremlin.structure.VertexProperty}
     * @param b the second {@link org.apache.tinkerpop.gremlin.structure.VertexProperty}
     * @return true if equal and false otherwise
     */
    public static boolean areEqual(final VertexProperty a, final Object b) {
        return ElementHelper.areEqual((Element) a, b);
    }

    /**
     * Simply tests if the value returned from {@link org.apache.tinkerpop.gremlin.structure.Element#id()} are {@code equal()}.
     *
     * @param a the first {@link org.apache.tinkerpop.gremlin.structure.Element}
     * @param b the second {@link org.apache.tinkerpop.gremlin.structure.Element}
     * @return true if ids are equal and false otherwise
     */
    public static boolean haveEqualIds(final Element a, final Element b) {
        return a.id().equals(b.id());
    }

    /**
     * If two {@link Element} instances are equal, then they must have the same hash codes. This methods ensures consistent hashCode values.
     *
     * @param element the element to get the hashCode for
     * @return the hash code of the element
     */
    public static int hashCode(final Element element) {
        return element.id().hashCode();
    }

    /**
     * If two {@link Property} instances are equal, then they must have the same hash codes. This methods ensures consistent hashCode values.
     * For {@link VertexProperty} use {@link ElementHelper#hashCode(org.apache.tinkerpop.gremlin.structure.Element)}.
     *
     * @param property the property to get the hashCode for
     * @return the hash code of the property
     */
    public static int hashCode(final Property property) {
        return property.key().hashCode() + property.value().hashCode();
    }

    /**
     * A standard method for determining if two {@link org.apache.tinkerpop.gremlin.structure.Property} objects are equal. This method should be used by any
     * {@link Object#equals(Object)} implementation to ensure consistent behavior.
     *
     * @param a the first {@link org.apache.tinkerpop.gremlin.structure.Property}
     * @param b the second {@link org.apache.tinkerpop.gremlin.structure.Property}
     * @return true if equal and false otherwise
     */
    public static boolean areEqual(final Property a, final Object b) {
        if (null == a)
            throw Graph.Exceptions.argumentCanNotBeNull("a");
        if (null == b)
            throw Graph.Exceptions.argumentCanNotBeNull("b");

        if (a == b)
            return true;
        if (!(b instanceof Property))
            return false;
        if (!a.isPresent() && !((Property) b).isPresent())
            return true;
        if (!a.isPresent() && ((Property) b).isPresent() || a.isPresent() && !((Property) b).isPresent())
            return false;
        return a.key().equals(((Property) b).key()) && a.value().equals(((Property) b).value()) && areEqual(a.element(), ((Property) b).element());

    }

    public static Map<String, Object> propertyValueMap(final Element element, final String... propertyKeys) {
        final Map<String, Object> values = new HashMap<>();
        element.properties(propertyKeys).forEachRemaining(property -> values.put(property.key(), property.value()));
        return values;
    }

    public static Map<String, Property> propertyMap(final Element element, final String... propertyKeys) {
        final Map<String, Property> propertyMap = new HashMap<>();
        element.properties(propertyKeys).forEachRemaining(property -> propertyMap.put(property.key(), property));
        return propertyMap;
    }

    public static Map<String, List> vertexPropertyValueMap(final Vertex vertex, final String... propertyKeys) {
        final Map<String, List> valueMap = new HashMap<>();
        vertex.properties(propertyKeys).forEachRemaining(property -> {
            if (valueMap.containsKey(property.key()))
                valueMap.get(property.key()).add(property.value());
            else {
                final List list = new ArrayList();
                list.add(property.value());
                valueMap.put(property.key(), list);
            }
        });
        return valueMap;
    }

    public static Map<String, List<VertexProperty>> vertexPropertyMap(final Vertex vertex, final String... propertyKeys) {
        final Map<String, List<VertexProperty>> propertyMap = new HashMap<>();
        vertex.properties(propertyKeys).forEachRemaining(property -> {
            if (propertyMap.containsKey(property.key()))
                propertyMap.get(property.key()).add(property);
            else {
                final List<VertexProperty> list = new ArrayList<>();
                list.add(property);
                propertyMap.put(property.key(), list);
            }
        });
        return propertyMap;
    }

    public static boolean keyExists(final String key, final String... providedKeys) {
        if (Graph.Hidden.isHidden(key)) return false;
        if (0 == providedKeys.length) return true;
        if (1 == providedKeys.length) return key.equals(providedKeys[0]);
        else {
            for (final String temp : providedKeys) {
                if (temp.equals(key))
                    return true;
            }
            return false;
        }
    }

    public static boolean idExists(final Object id, final Object... providedIds) {
        if (0 == providedIds.length) return true;

        // it is OK to evaluate equality of ids via toString() now given that the toString() the test suite
        // enforces the value of id.()toString() to be a first class representation of the identifier
        if (1 == providedIds.length) return id.toString().equals(providedIds[0].toString());
        else {
            for (final Object temp : providedIds) {
                if (temp.toString().equals(id.toString()))
                    return true;
            }
            return false;
        }
    }
}