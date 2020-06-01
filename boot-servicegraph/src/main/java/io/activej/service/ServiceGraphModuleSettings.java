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

package io.activej.service;

import io.activej.di.Key;

@SuppressWarnings("unused")
public interface ServiceGraphModuleSettings {
	<T> ServiceGraphModule register(Class<? extends T> type, ServiceAdapter<T> factory);

	<T> ServiceGraphModule registerForSpecificKey(Key<T> key, ServiceAdapter<T> factory);

	<T> ServiceGraphModule excludeSpecificKey(Key<T> key);

	ServiceGraphModule addDependency(Key<?> key, Key<?> keyDependency);

	ServiceGraphModule removeDependency(Key<?> key, Key<?> keyDependency);
}
