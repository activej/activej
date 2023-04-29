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

package io.activej.rpc.server;

import io.activej.promise.Promise;

/**
 * Implementations of this interface specifies the behavior according to
 * business logic and passes result to callback.
 * <p>
 * An example of concrete {@code RpcRequestHandler} can be found in
 * {@link RpcServer} documentation.
 *
 * @param <I> class of request
 * @param <O> class of response
 */
@FunctionalInterface
public interface RpcRequestHandler<I, O> {
	Promise<O> run(I request);
}
