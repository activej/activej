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
import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.attributes.AttributeResolver;
import io.activej.cube.attributes.AttributeResolver.AttributesFunction;
import io.activej.cube.attributes.AttributeResolver.KeyFunction;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.promise.Promise;

import java.util.*;

import static io.activej.codegen.expression.Expressions.*;
import static java.util.stream.Collectors.toSet;

public final class Utils {

	public static <R> Class<R> createResultClass(Collection<String> attributes, Collection<String> measures,
			Cube cube, DefiningClassLoader classLoader) {
		ClassBuilder<R> builder = ClassBuilder.create(classLoader, Object.class);
		builder.withClassKey(new HashSet<>(attributes), new HashSet<>(measures));
		for (String attribute : attributes) {
			builder.withField(attribute.replace('.', '$'), cube.getAttributeInternalType(attribute));
		}
		for (String measure : measures) {
			builder.withField(measure, cube.getMeasureInternalType(measure));
		}
		return builder.build();
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
	public static <R> Promise<Void> resolveAttributes(List<R> results, AttributeResolver attributeResolver,
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
		KeyFunction keyFunction = ClassBuilder.create(classLoader, KeyFunction.class)
				.withClassKey(recordClass, recordDimensions, Arrays.asList(fullySpecifiedDimensionsArray))
				.withMethod("extractKey",
						let(
								arrayNew(Object[].class, value(recordDimensions.size())),
								key -> sequence(expressions -> {
									for (int i = 0; i < recordDimensions.size(); i++) {
										String dimension = recordDimensions.get(i);
										expressions.add(arraySet(key, value(i),
												fullySpecifiedDimensions.containsKey(dimension) ?
														arrayGet(value(fullySpecifiedDimensionsArray), value(i)) :
														cast(property(cast(arg(0), recordClass), dimension), Object.class)));
									}
									expressions.add(key);
								})))
				.buildClassAndCreateNewInstance();

		List<String> resolverAttributes = new ArrayList<>(attributeResolver.getAttributeTypes().keySet());
		AttributesFunction attributesFunction = ClassBuilder.create(classLoader, AttributesFunction.class)
				.withClassKey(recordClass, new HashSet<>(recordAttributes))
				.withMethod("applyAttributes",
						sequence(expressions -> {
							for (String attribute : recordAttributes) {
								String attributeName = attribute.substring(attribute.indexOf('.') + 1);
								int resolverAttributeIndex = resolverAttributes.indexOf(attributeName);
								expressions.add(set(
										property(cast(arg(0), recordClass), attribute.replace('.', '$')),
										arrayGet(arg(1), value(resolverAttributeIndex))));
							}
						}))
				.buildClassAndCreateNewInstance();

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
