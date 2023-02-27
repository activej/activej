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

import io.activej.inject.Key;
import io.activej.service.adapter.ServiceAdapter;

@SuppressWarnings("unused")
public interface ServiceGraphModuleSettings {
	<T> ServiceGraphModuleSettings with(Class<? extends T> type, ServiceAdapter<T> factory);

	<T> ServiceGraphModuleSettings withKey(Key<T> key, ServiceAdapter<T> factory);

	<T> ServiceGraphModuleSettings withExcludedKey(Key<T> key);

	ServiceGraphModuleSettings withDependency(Key<?> key, Key<?> keyDependency);

	ServiceGraphModuleSettings withExcludedDependency(Key<?> key, Key<?> keyDependency);
}
