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

package io.activej.http.inject;

import io.activej.common.exception.UncheckedException;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

import static java.util.Arrays.asList;

public class ScopeServlet implements AsyncServlet {
	public static final Scope REQUEST_SCOPE = Scope.of(RequestScope.class);

	public static final Key<HttpRequest> HTTP_REQUEST_KEY = new Key<HttpRequest>() {};
	public static final Key<Promise<HttpResponse>> HTTP_RESPONSE_KEY = new Key<Promise<HttpResponse>>() {};

	private final Injector injector;

	protected ScopeServlet(Injector injector, Module... modules) {
		this(injector, asList(modules));
	}

	protected ScopeServlet(Injector injector, Collection<Module> modules) {
		this.injector = Injector.of(injector,
				Modules.combine(modules),
				getModule(),
				new AbstractModule() {
					@Override
					protected void configure() {
						// so anonymous servlet subclasses could use the DSL
						scan(ScopeServlet.this);
						// dummy binding to be replaced by subInjector.putInstance
						bind(HTTP_REQUEST_KEY).in(REQUEST_SCOPE).to(() -> {
							throw new AssertionError();
						});
						// make sure that response is provided or generated in request scope
						bind(HTTP_RESPONSE_KEY).in(REQUEST_SCOPE);
					}
				}
		);
	}

	protected Module getModule() {
		return PromiseGeneratorModule.create();
	}

	@Override
	public @NotNull Promise<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		Injector subInjector = injector.enterScope(REQUEST_SCOPE);
		subInjector.putInstance(HTTP_REQUEST_KEY, request);
		return subInjector.getInstance(HTTP_RESPONSE_KEY);
	}
}
