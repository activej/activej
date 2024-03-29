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

package io.activej.csp.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.recycle.Recyclable;
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class Recycling<T extends Recyclable> extends AbstractChannelConsumer<T> {

	@Override
	protected Promise<Void> doAccept(@Nullable T value) {
		if (value != null) {
			value.recycle();
		}
		return Promise.complete();
	}
}
