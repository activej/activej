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

package io.activej.inject.module;

import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

@SuppressWarnings("ClassExplicitlyAnnotation")
public final class UniqueQualifierImpl implements UniqueQualifier {
	@Nullable
	private final Object originalQualifier;

	public UniqueQualifierImpl() {
		this.originalQualifier = null;
	}

	public UniqueQualifierImpl(@Nullable Object originalQualifier) {
		this.originalQualifier = originalQualifier;
	}

	@Nullable
	public Object getOriginalQualifier() {
		return originalQualifier;
	}

	@Override
	public Class<? extends Annotation> annotationType() {
		return UniqueQualifier.class;
	}

	@Override
	public String toString() {
		return "@" + Integer.toHexString(hashCode()) + (originalQualifier != null ? " " + originalQualifier : "");
	}
}
