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

package io.activej.net;

import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;

import java.io.IOException;

/**
 * Represents non-blocking server which listens to new connections and accepts them asynchronously.
 * It operates on eventloop in eventloop thread and uses eventloop integration with Java NIO.
 */
public interface NioReactorServer {
	NioReactor getReactor();

	/**
	 * Tells this server to start listening on its listen addresses.
	 *
	 * @throws IOException if the socket can not be created.
	 */
	void listen() throws IOException;

	/**
	 * Closes the server.
	 * Any open channels will be closed.
	 */
	Promise<?> close();
}
