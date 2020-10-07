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
import io.activej.serializer.impl.SerializerDefWithVarLength;

public final class SerializeVarLengthHandler implements AnnotationHandler<SerializeVarLength, SerializeVarLengthEx> {
	@Override
	public SerializerDefBuilder createBuilder(Context context, SerializeVarLength annotation) {
		return (type, generics, target) -> {
			if (generics.length != 0)
				throw new IllegalArgumentException("Type should have no generics");
			if (target instanceof SerializerDefWithVarLength) {
				return ((SerializerDefWithVarLength) target).ensureVarLength();
			}
			throw new IllegalArgumentException("Unsupported type " + type);
		};
	}

	@Override
	public int[] extractPath(SerializeVarLength annotation) {
		return annotation.path();
	}

	@Override
	public SerializeVarLength[] extractList(SerializeVarLengthEx plural) {
		return plural.value();
	}
}
