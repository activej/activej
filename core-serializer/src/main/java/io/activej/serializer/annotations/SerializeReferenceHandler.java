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

import io.activej.serializer.impl.SerializerDefBuilder;
import io.activej.serializer.impl.SerializerDefReference;

public final class SerializeReferenceHandler implements AnnotationHandler<SerializeReference, SerializeReferenceEx> {
	@Override
	public SerializerDefBuilder createBuilder(Context context, SerializeReference annotation) {
		return (type, generics, target) -> {
			if (type.isPrimitive())
				throw new IllegalArgumentException("Type must not represent a primitive type");
			return new SerializerDefReference(target);
		};
	}

	@Override
	public int[] extractPath(SerializeReference annotation) {
		return annotation.path();
	}

	@Override
	public SerializeReference[] extractList(SerializeReferenceEx plural) {
		return plural.value();
	}
}
