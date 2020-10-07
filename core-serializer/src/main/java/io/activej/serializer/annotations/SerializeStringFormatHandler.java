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
import io.activej.serializer.StringFormat;
import io.activej.serializer.impl.SerializerDefBuilder;
import io.activej.serializer.impl.SerializerDefString;

public class SerializeStringFormatHandler implements AnnotationHandler<SerializeStringFormat, SerializeStringFormatEx> {
	@SuppressWarnings("deprecation") // compatibility
	@Override
	public SerializerDefBuilder createBuilder(Context context, SerializeStringFormat annotation) {
		return (type, generics, target) -> {
			if (context.getCompatibilityLevel() == CompatibilityLevel.LEVEL_1) {
				if (annotation.value() == StringFormat.ISO_8859_1 || annotation.value() == StringFormat.UTF8) {
					return ((SerializerDefString) target).encoding(StringFormat.UTF8_MB3);
				}
			}
			return ((SerializerDefString) target).encoding(annotation.value());
		};
	}

	@Override
	public int[] extractPath(SerializeStringFormat annotation) {
		return annotation.path();
	}

	@Override
	public SerializeStringFormat[] extractList(SerializeStringFormatEx plural) {
		return plural.value();
	}
}
