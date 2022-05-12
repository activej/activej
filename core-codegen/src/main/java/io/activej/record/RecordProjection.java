package io.activej.record;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;

public abstract class RecordProjection implements UnaryOperator<Record>, BiConsumer<Record, Record> {
	private final RecordScheme schemeFrom;
	private final RecordScheme schemeTo;

	protected RecordProjection(RecordScheme from, RecordScheme to) {
		this.schemeFrom = from;
		this.schemeTo = to;
	}

	public static RecordProjection projection(RecordScheme schemeFrom, String... fields) {
		return projection(schemeFrom, List.of(fields));
	}

	public static RecordProjection projection(RecordScheme schemeFrom, Collection<String> fields) {
		return projection(DefiningClassLoader.create(schemeFrom.getClassLoader()), schemeFrom, fields);
	}

	public static RecordProjection projection(DefiningClassLoader classLoader, RecordScheme schemeFrom, String... fields) {
		return projection(classLoader, schemeFrom, List.of(fields));
	}

	public static RecordProjection projection(DefiningClassLoader classLoader, RecordScheme schemeFrom, Collection<String> fields) {
		if (!isParentChild(schemeFrom.getClassLoader(), classLoader))
			throw new IllegalArgumentException("Unrelated ClassLoaders");
		RecordScheme schemeTo = RecordScheme.create(classLoader);
		Map<String, UnaryOperator<Expression>> mapping = new HashMap<>();
		for (String field : schemeFrom.getFields()) {
			Type fieldType = schemeFrom.getFieldType(field);
			if (fields.contains(field)) {
				schemeTo.withField(field, fieldType);
				mapping.put(field, recordFrom -> schemeFrom.property(recordFrom, field));
			}
		}
		return projection(classLoader, List.of(schemeFrom, fields), schemeFrom, schemeTo, mapping);
	}

	public static RecordProjection projection(RecordScheme schemeFrom, RecordScheme schemeTo,
			Map<String, UnaryOperator<Expression>> mapping) {
		DefiningClassLoader classLoaderChild = getClassLoaderChild(schemeFrom.getClassLoader(), schemeTo.getClassLoader());
		return projection(DefiningClassLoader.create(classLoaderChild), null, schemeFrom, schemeTo, mapping);
	}

	public static @NotNull RecordProjection projection(DefiningClassLoader classLoader, @Nullable Object classKey,
			RecordScheme schemeFrom, RecordScheme schemeTo,
			Map<String, UnaryOperator<Expression>> mapping) {
		schemeFrom.build();
		schemeTo.build();
		return classLoader.ensureClassAndCreateInstance(
				ClassKey.of(RecordProjection.class, classKey),
				() -> ClassBuilder.create(RecordProjection.class)
						.withConstructor(List.of(RecordScheme.class, RecordScheme.class),
								superConstructor(arg(0), arg(1)))
						.withMethod("accept", void.class, List.of(Record.class, Record.class), sequence(seq -> {
							for (Map.Entry<String, UnaryOperator<Expression>> entry : mapping.entrySet()) {
								seq.add(Expressions.set(
										schemeTo.property(cast(arg(1), schemeTo.getRecordClass()), entry.getKey()),
										entry.getValue().apply(cast(arg(0), schemeFrom.getRecordClass()))
								));
							}
						})),
				schemeFrom, schemeTo);
	}

	private static DefiningClassLoader getClassLoaderChild(DefiningClassLoader classLoader1, DefiningClassLoader classLoader2) {
		if (isParentChild(classLoader1, classLoader2)) {
			return classLoader2;
		} else if (isParentChild(classLoader2, classLoader1)) {
			return classLoader1;
		} else {
			throw new IllegalArgumentException("Unrelated ClassLoaders");
		}
	}

	private static boolean isParentChild(DefiningClassLoader maybeParent, DefiningClassLoader maybeChild) {
		for (ClassLoader cl = maybeChild; cl != null; cl = cl.getParent()) {
			if (cl == maybeParent) {
				return true;
			}
		}
		return false;
	}

	public final RecordScheme getSchemeFrom() {
		return schemeFrom;
	}

	public final RecordScheme getSchemeTo() {
		return schemeTo;
	}

	@Override
	public Record apply(Record recordFrom) {
		Record recordTo = schemeTo.record();
		accept(recordFrom, recordTo);
		return recordTo;
	}

	@Override
	public abstract void accept(Record recordFrom, Record recordTo);
}
