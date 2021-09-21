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

package io.activej.common.inspector;

import org.jetbrains.annotations.Nullable;

public interface BaseInspector<I extends BaseInspector<I>> {
	<T extends I> @Nullable T lookup(Class<T> type);

	static <I extends BaseInspector<I>, T extends I> T lookup(@Nullable BaseInspector<I> inspector, Class<T> type) {
		return inspector != null ? inspector.lookup(type) : null;
	}
}
