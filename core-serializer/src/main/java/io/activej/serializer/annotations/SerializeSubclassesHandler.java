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

import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerBuilder.Helper;
import io.activej.serializer.impl.SerializerDefBuilder;

public final class SerializeSubclassesHandler implements AnnotationHandler<SerializeSubclasses, SerializeSubclassesEx> {
	@Override
	public SerializerDefBuilder createBuilder(Helper serializerBuilder, SerializeSubclasses annotation, CompatibilityLevel compatibilityLevel) {
		return (superclass, superclassGenerics, target) -> {
			if (superclass.getTypeParameters().length != 0)
				throw new IllegalArgumentException("Superclass must have no type parameters");
			if (superclassGenerics.length != 0)
				throw new IllegalArgumentException("Superclass must have no generics");

			return serializerBuilder.createSubclassesSerializer(superclass, annotation);
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
