/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.cube;

import io.activej.codegen.ClassGenerator;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.attributes.IAttributeResolver;
import io.activej.cube.attributes.IAttributeResolver.AttributesFunction;
import io.activej.cube.attributes.IAttributeResolver.KeyFunction;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.promise.Promise;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.codegen.expression.Expressions.*;

public final class Utils {

	public static <R> Class<R> createResultClass(
		Collection<String> attributes, Collection<String> measures, CubeStructure structure, DefiningClassLoader classLoader
	) {
		return classLoader.ensureClass(
			ClassKey.of(Object.class, new HashSet<>(attributes), new HashSet<>(measures)),
			() -> {
				//noinspection unchecked
				return ClassGenerator.builder((Class<R>) Object.class)
					.initialize(b -> {
						for (String attribute : attributes) {
							b.withField(attribute.replace('.', '$'), structure.getAttributeInternalType(attribute));
						}
						for (String measure : measures) {
							b.withField(measure, structure.getMeasureInternalType(measure));
						}
					})
					.build();
			}
		);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static <R> Promise<Void> resolveAttributes(
		List<R> results, IAttributeResolver attributeResolver, List<String> recordDimensions,
		List<String> recordAttributes, Map<String, Object> fullySpecifiedDimensions, Class<R> recordClass,
		CubeStructure structure, DefiningClassLoader classLoader
	) {
		Object[] fullySpecifiedDimensionsArray = new Object[recordDimensions.size()];
		for (int i = 0; i < recordDimensions.size(); i++) {
			String dimension = recordDimensions.get(i);
			if (fullySpecifiedDimensions.containsKey(dimension)) {
				FieldType fieldType = structure.getDimensionTypes().get(dimension);
				fullySpecifiedDimensionsArray[i] = fieldType.toInternalValue(fullySpecifiedDimensions.get(dimension));
			}
		}

		KeyFunction keyFunction = classLoader.ensureClassAndCreateInstance(
			ClassKey.of(KeyFunction.class, recordClass, recordDimensions, Arrays.asList(fullySpecifiedDimensionsArray)),
			() -> ClassGenerator.builder(KeyFunction.class)
				.withMethod("extractKey",
					let(
						arrayNew(Object[].class, value(recordDimensions.size())),
						key -> sequence(seq -> {
							for (int i = 0; i < recordDimensions.size(); i++) {
								String dimension = recordDimensions.get(i);
								seq.add(arraySet(key, value(i),
									fullySpecifiedDimensions.containsKey(dimension) ?
										arrayGet(value(fullySpecifiedDimensionsArray), value(i)) :
										cast(property(cast(arg(0), recordClass), dimension), Object.class)));
							}
							return key;
						})))
				.build()
		);

		AttributesFunction attributesFunction = classLoader.ensureClassAndCreateInstance(
			ClassKey.of(AttributesFunction.class, recordClass, new HashSet<>(recordAttributes)),
			() -> ClassGenerator.builder(AttributesFunction.class)
				.withMethod("applyAttributes",
					sequence(seq -> {
						List<String> resolverAttributes = new ArrayList<>(attributeResolver.getAttributeTypes().keySet());
						for (String attribute : recordAttributes) {
							String attributeName = attribute.substring(attribute.indexOf('.') + 1);
							int resolverAttributeIndex = resolverAttributes.indexOf(attributeName);
							seq.add(set(
								property(cast(arg(0), recordClass), attribute.replace('.', '$')),
								arrayGet(arg(1), value(resolverAttributeIndex))));
						}
					}))
				.build()
		);

		return attributeResolver.resolveAttributes((List<Object>) results, keyFunction, attributesFunction);
	}

	public static <D> Set<Long> chunksInDiffs(CubeDiffScheme<D> cubeDiffsExtractor, List<? extends D> diffs) {
		return diffs.stream()
			.flatMap(cubeDiffsExtractor::unwrapToStream)
			.flatMapToLong(CubeDiff::addedChunks)
			.boxed()
			.collect(Collectors.toSet());
	}

	public static <K, V> Stream<Map.Entry<K, V>> filterEntryKeys(Stream<Map.Entry<K, V>> stream, Predicate<K> predicate) {
		return stream.filter(entry -> predicate.test(entry.getKey()));
	}

}
