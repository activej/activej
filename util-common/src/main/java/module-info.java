module io.activej.common {
	requires transitive org.jetbrains.annotations;
	requires transitive org.slf4j;

	exports io.activej.common;
	exports io.activej.common.annotation;
	exports io.activej.common.builder;
	exports io.activej.common.collection;
	exports io.activej.common.exception;
	exports io.activej.common.function;
	exports io.activej.common.initializer;
	exports io.activej.common.inspector;
	exports io.activej.common.recycle;
	exports io.activej.common.ref;
	exports io.activej.common.reflection;
	exports io.activej.common.service;
	exports io.activej.common.time;
	exports io.activej.common.tuple;
}
