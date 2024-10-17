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

package io.activej.test.rules;

import io.activej.bytebuf.ByteBufPool;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.junit.Assert.assertEquals;

/**
 * {@link TestRule} that fails if not all byte buffers requested from the {@link ByteBufPool} were recycled properly.
 * <p>
 * Annotation {@link IgnoreLeaks} can be put on a test that wants this rule disabled.
 */
public final class ByteBufRule implements TestRule {
	static {
		System.setProperty("ByteBufPool.stats", "true");
		System.setProperty("ByteBufPool.registry", "true");
		System.setProperty("ByteBufPool.minSize", "0");
		System.setProperty("ByteBufPool.maxSize", "0");
		System.setProperty("ByteBufPool.clearOnRecycle", "true");
		System.setProperty("ObjectPool.initialCapacity", "1024");
	}

	@Override
	public Statement apply(Statement base, Description description) {
		if (description.getTestClass().getAnnotation(IgnoreLeaks.class) != null
			|| description.getAnnotation(IgnoreLeaks.class) != null) {
			return base;
		}
		return new LambdaStatement(() -> {
			ByteBufPool.clear();
			base.evaluate();
			assertEquals(ByteBufPool.getStats().getPoolItemsString(), ByteBufPool.getStats().getCreatedItems(), ByteBufPool.getStats().getPoolItems());
		});
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.TYPE})
	public @interface IgnoreLeaks {

		/**
		 * An optional description for why the test needs to ignore leaks
		 */
		String value() default "";
	}
}
