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

package io.activej.datastream.dsl;

import io.activej.datastream.StreamSupplier;
import io.activej.reactor.Reactive;

/**
 * A separate interface for components that have some kind of output represented with a {@link StreamSupplier}.
 */
public interface HasStreamOutput<O> extends Reactive {
	StreamSupplier<O> getOutput();
}
