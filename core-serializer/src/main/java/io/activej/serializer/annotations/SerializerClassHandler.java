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
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.SerializerDefBuilder;

import java.lang.reflect.InvocationTargetException;

public final class SerializerClassHandler implements AnnotationHandler<SerializerClass, SerializerClassEx> {
	@Override
	public SerializerDefBuilder createBuilder(Helper serializerBuilder, SerializerClass annotation, CompatibilityLevel compatibilityLevel) {
		try {
			SerializerDef serializer = annotation.value().newInstance();
			return SerializerDefBuilder.of(serializer);
		} catch (InstantiationException | IllegalAccessException e) {
			try {
				SerializerDef serializer = (SerializerDef) annotation.value().getMethod("instance").invoke(null);
				return SerializerDefBuilder.of(serializer);
			} catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException ignored) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public int[] extractPath(SerializerClass annotation) {
		return annotation.path();
	}

	@Override
	public SerializerClass[] extractList(SerializerClassEx plural) {
		return plural.value();
	}
}
