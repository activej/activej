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

package io.activej.di.annotation;

import io.activej.di.Key;
import io.activej.di.util.Utils;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a helper annotation that can be used to override how this type is {@link Key#getDisplayString() displayed}
 * in error messages and {@link Utils#makeGraphVizGraph debug graphs}.
 * <p>
 * Packages and enclosing classes are stripped off for readability,
 * but if you have multiple types with the same name in different packages
 * you can override their display name by applying this annotation.
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface ShortTypeName {
	String value();
}

