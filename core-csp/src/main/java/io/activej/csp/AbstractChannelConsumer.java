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

package io.activej.csp;

import io.activej.async.process.AbstractAsyncCloseable;
import io.activej.async.process.AsyncCloseable;
import io.activej.common.Checks;
import io.activej.common.recycle.Recyclers;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import static io.activej.reactor.Reactive.checkInReactorThread;

public abstract class AbstractChannelConsumer<T> extends AbstractAsyncCloseable implements ChannelConsumer<T> {
	private static final boolean CHECK = Checks.isEnabled(AbstractChannelConsumer.class);

	// region creators
	protected AbstractChannelConsumer() {
		setCloseable(null);
	}

	protected AbstractChannelConsumer(@Nullable AsyncCloseable closeable) {
		setCloseable(closeable);
	}
	// endregion

	protected abstract Promise<Void> doAccept(@Nullable T value);

	@Override
	public final Promise<Void> accept(@Nullable T value) {
		if (CHECK) checkInReactorThread(this);
		if (isClosed()) {
			Recyclers.recycle(value);
			return Promise.ofException(getException());
		}
		return doAccept(value);
	}
}
