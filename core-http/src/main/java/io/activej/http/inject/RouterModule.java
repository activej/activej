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

import io.activej.http.AsyncServlet;
import io.activej.http.HttpMethod;
import io.activej.http.RoutingServlet;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.inject.module.AbstractModule;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class RouterModule extends AbstractModule {

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	protected void configure() {
		List<Key<? extends AsyncServlet>> mappedKeys = new ArrayList<>();

		transform(0, (bindings, scope, key, binding) -> {
			if (scope.length == 0 && key.getQualifier() instanceof Mapped && AsyncServlet.class.isAssignableFrom(key.getRawType())) {
				mappedKeys.add((Key<AsyncServlet>) (Key) key);
			}
			return binding;
		});

		bind(AsyncServlet.class)
				.qualified(Router.class)
				.to(injector -> {
					RoutingServlet router = RoutingServlet.create();
					for (Key<? extends AsyncServlet> key : mappedKeys) {
						AsyncServlet servlet = injector.getInstance(key);
						Mapped mapped = (Mapped) key.getQualifier();
						assert mapped != null;
						router.map(mapped.method().getHttpMethod(), mapped.value(), servlet);
					}
					return router;
				}, Injector.class);
	}

	@QualifierAnnotation
	@Target({FIELD, PARAMETER, METHOD})
	@Retention(RUNTIME)
	public @interface Mapped {
		MappedHttpMethod method() default MappedHttpMethod.ALL;

		String value();
	}

	@QualifierAnnotation
	@Target({FIELD, PARAMETER, METHOD})
	@Retention(RUNTIME)
	public @interface Router {
	}

	public enum MappedHttpMethod {
		GET, PUT, POST, HEAD, DELETE, CONNECT, OPTIONS, TRACE, PATCH,
		SEARCH, COPY, MOVE, LOCK, UNLOCK, MKCOL, PROPFIND, PROPPATCH,

		ALL;

		@Nullable
		HttpMethod getHttpMethod() {
			return this == ALL ? null : HttpMethod.values()[ordinal()];
		}
	}
}
