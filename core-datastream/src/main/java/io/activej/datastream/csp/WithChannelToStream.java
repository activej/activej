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

package io.activej.datastream.csp;

import io.activej.csp.dsl.ChannelSupplierTransformer;
import io.activej.csp.dsl.WithChannelInput;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.datastream.StreamSupplier;

/**
 * This interface is a shortcut for implementing transformers that convert
 * {@link ChannelSupplier channel suppliers} to {@link StreamSupplier stream suppliers}
 * and are useful through both DSLs.
 */
public interface WithChannelToStream<Self, I, O> extends
		StreamSupplier<O>,
		WithChannelInput<Self, I>,
		ChannelSupplierTransformer<I, StreamSupplier<O>> {

	@Override
	default StreamSupplier<O> transform(ChannelSupplier<I> supplier) {
		getInput().set(supplier);
		return this;
	}
}
