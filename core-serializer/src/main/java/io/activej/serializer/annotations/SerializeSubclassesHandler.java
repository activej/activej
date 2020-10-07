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

package io.activej.serializer.annotations;

import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.SerializerDefBuilder;
import io.activej.serializer.impl.SerializerDefSubclass;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import static java.lang.String.format;
import static java.util.Collections.emptyList;

public final class SerializeSubclassesHandler implements AnnotationHandler<SerializeSubclasses, SerializeSubclassesEx> {
	@Override
	public SerializerDefBuilder createBuilder(Context context, SerializeSubclasses annotation) {
		return (superclass, superclassGenerics, target) -> {
			if (superclass.getTypeParameters().length != 0)
				throw new IllegalArgumentException("Superclass must have no type parameters");
			if (superclassGenerics.length != 0)
				throw new IllegalArgumentException("Superclass must have no generics");

			LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>(Arrays.asList(annotation.value()));
			if (subclassesSet.size() != annotation.value().length)
				throw new IllegalArgumentException("Subclasses should be unique");

			if (!annotation.extraSubclassesId().isEmpty()) {
				Collection<Class<?>> registeredSubclasses = context.getExtraSubclassesMap().get(annotation.extraSubclassesId());
				if (registeredSubclasses != null) {
					subclassesSet.addAll(registeredSubclasses);
				}
			}

			if (subclassesSet.isEmpty()) throw new IllegalArgumentException("Set of subclasses should not be empty");
			LinkedHashMap<Class<?>, SerializerDef> subclasses = new LinkedHashMap<>();
			for (Class<?> subclass : subclassesSet) {
				if (subclass.getTypeParameters().length != 0)
					throw new IllegalArgumentException("Subclass should have no type parameters");
				if (!superclass.isAssignableFrom(subclass))
					throw new IllegalArgumentException(format("Unrelated subclass '%s' for '%s'", subclass, superclass));

				subclasses.put(subclass, context.createSerializerDef(
						subclass,
						new SerializerDefBuilder.SerializerForType[]{},
						emptyList()));
			}
			return new SerializerDefSubclass(superclass, subclasses, annotation.startIndex());
		};
	}

	@Override
	public int[] extractPath(SerializeSubclasses annotation) {
		return annotation.path();
	}

	@Override
	public SerializeSubclasses[] extractList(SerializeSubclassesEx plural) {
		return plural.value();
	}
}
