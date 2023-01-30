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

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.attributes.IAttributeResolver;
import io.activej.cube.attributes.IAttributeResolver.AttributesFunction;
import io.activej.cube.attributes.IAttributeResolver.KeyFunction;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.promise.Promise;

import java.util.*;

import static io.activej.codegen.expression.Expressions.*;
import static java.util.stream.Collectors.toSet;

public final class Utils {

	public static <R> Class<R> createResultClass(Collection<String> attributes, Collection<String> measures,
			Cube cube, DefiningClassLoader classLoader) {
		return classLoader.ensureClass(
				ClassKey.of(Object.class, new HashSet<>(attributes), new HashSet<>(measures)),
				() -> {
					//noinspection unchecked
					ClassBuilder<R>.Builder builder = ClassBuilder.builder((Class<R>) Object.class);
					for (String attribute : attributes) {
						builder.withField(attribute.replace('.', '$'), cube.getAttributeInternalType(attribute));
					}
					for (String measure : measures) {
						builder.withField(measure, cube.getMeasureInternalType(measure));
					}
					return builder.build();
				}
		);
	}

	static boolean startsWith(List<String> list, List<String> prefix) {
		if (prefix.size() >= list.size())
			return false;

		for (int i = 0; i < prefix.size(); ++i) {
			if (!list.get(i).equals(prefix.get(i)))
				return false;
		}

		return true;
	}

	@SuppressWarnings("unchecked")
	public static <R> Promise<Void> resolveAttributes(List<R> results, IAttributeResolver attributeResolver,
			List<String> recordDimensions, List<String> recordAttributes,
			Map<String, Object> fullySpecifiedDimensions,
			Class<R> recordClass, DefiningClassLoader classLoader) {
		Object[] fullySpecifiedDimensionsArray = new Object[recordDimensions.size()];
		for (int i = 0; i < recordDimensions.size(); i++) {
			String dimension = recordDimensions.get(i);
			if (fullySpecifiedDimensions.containsKey(dimension)) {
				fullySpecifiedDimensionsArray[i] = fullySpecifiedDimensions.get(dimension);
			}
		}

		KeyFunction keyFunction = classLoader.ensureClassAndCreateInstance(
				ClassKey.of(KeyFunction.class, recordClass, recordDimensions, Arrays.asList(fullySpecifiedDimensionsArray)),
				() -> ClassBuilder.builder(KeyFunction.class)
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
				() -> ClassBuilder.builder(AttributesFunction.class)
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

	@SuppressWarnings("unchecked")
	public static <D, C> Set<C> chunksInDiffs(CubeDiffScheme<D> cubeDiffsExtractor,
			List<? extends D> diffs) {
		return diffs.stream()
				.flatMap(cubeDiffsExtractor::unwrapToStream)
				.flatMap(CubeDiff::addedChunks)
				.map(id -> (C) id)
				.collect(toSet());
	}
}
