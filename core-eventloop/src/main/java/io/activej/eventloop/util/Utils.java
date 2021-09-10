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

package io.activej.eventloop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.channels.Selector;

/**
 * Is used to replace the inefficient {@link java.util.HashSet} in {@link sun.nio.ch.SelectorImpl}
 * into {@link OptimizedSelectedKeysSet}. Replace fields to advance performance for the {@link Selector}
 */
public final class Utils {
	private static final Logger logger = LoggerFactory.getLogger(Utils.class);

	private static Field SELECTED_KEYS_FIELD;
	private static Field PUBLIC_SELECTED_KEYS_FIELD;

	static {
		try {
			Class<?> cls = Class.forName("sun.nio.ch.SelectorImpl", false, Thread.currentThread().getContextClassLoader());
			SELECTED_KEYS_FIELD = cls.getDeclaredField("selectedKeys");
			PUBLIC_SELECTED_KEYS_FIELD = cls.getDeclaredField("publicSelectedKeys");
			SELECTED_KEYS_FIELD.setAccessible(true);
			PUBLIC_SELECTED_KEYS_FIELD.setAccessible(true);
		} catch (ClassNotFoundException | NoSuchFieldException e) {
			logger.warn("Failed reflecting NIO selector fields", e);
		}
	}

	/**
	 * Replaces the selected keys field from {@link java.util.HashSet} in {@link sun.nio.ch.SelectorImpl}
	 * to {@link OptimizedSelectedKeysSet} to avoid overhead which causes a work of GC
	 *
	 * @param selector selector instance whose selected keys field is to be changed
	 * @return <code>true</code> on success
	 */
	public static boolean tryToOptimizeSelector(Selector selector) {
		OptimizedSelectedKeysSet selectedKeys = new OptimizedSelectedKeysSet();
		try {
			SELECTED_KEYS_FIELD.set(selector, selectedKeys);
			PUBLIC_SELECTED_KEYS_FIELD.set(selector, selectedKeys);
			return true;

		} catch (IllegalAccessException e) {
			logger.warn("Failed setting optimized set into selector", e);
		}

		return false;
	}
}
