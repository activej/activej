module io.activej.inject {
	requires transitive io.activej.common;
	requires transitive io.activej.types;

	exports io.activej.inject;
	exports io.activej.inject.annotation;
	exports io.activej.inject.binding;
	exports io.activej.inject.impl;
	exports io.activej.inject.module;
	exports io.activej.inject.util;
}
