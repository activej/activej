package io.activej.record;

import io.activej.codegen.ClassGenerator;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.codegen.expression.Variable;
import org.jetbrains.annotations.Nullable;

import java.util.*;
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
		RecordScheme.Builder schemeToBuilder = RecordScheme.builder(classLoader);
		Map<String, UnaryOperator<Expression>> mapping = new HashMap<>();
		List<String> fromFields = schemeFrom.getFields();
		for (String field : fields) {
			if (!fromFields.contains(field)) {
				throw new IllegalArgumentException("Unknown field: " + field);
			}
			schemeToBuilder.withField(field, schemeFrom.getFieldType(field));
			mapping.put(field, recordFrom -> schemeFrom.property(recordFrom, field));
		}
		List<String> comparedFields = schemeFrom.getComparatorFields();
		if (comparedFields != null) {
			List<String> newComparedFields = new ArrayList<>();
			for (String field : fields) {
				if (comparedFields.contains(field)) {
					newComparedFields.add(field);
				}
			}
			schemeToBuilder.withComparatorFields(newComparedFields);
		}
		RecordScheme schemeTo = schemeToBuilder.build();
		return projection(classLoader, List.of(schemeFrom, fields), schemeFrom, schemeTo, mapping);
	}

	public static RecordProjection projection(
		RecordScheme schemeFrom, RecordScheme schemeTo, Map<String, UnaryOperator<Expression>> mapping
	) {
		DefiningClassLoader classLoaderChild = getClassLoaderChild(schemeFrom.getClassLoader(), schemeTo.getClassLoader());
		return projection(DefiningClassLoader.create(classLoaderChild), null, schemeFrom, schemeTo, mapping);
	}

	public static RecordProjection projection(
		DefiningClassLoader classLoader, @Nullable Object classKey, RecordScheme schemeFrom, RecordScheme schemeTo,
		Map<String, UnaryOperator<Expression>> mapping
	) {
		return classLoader.ensureClassAndCreateInstance(
			ClassKey.of(RecordProjection.class, classKey),
			() -> ClassGenerator.builder(RecordProjection.class)
				.withConstructor(List.of(RecordScheme.class, RecordScheme.class),
					superConstructor(arg(0), arg(1)))
				.withMethod("accept", void.class, List.of(Record.class, Record.class),
					let(List.of(
							cast(arg(0), schemeFrom.getRecordClass()),
							cast(arg(1), schemeTo.getRecordClass())
						),
						variables -> sequence(seq -> {
							Variable recordFrom = variables[0];
							Variable recordTo = variables[1];

							for (Map.Entry<String, UnaryOperator<Expression>> entry : mapping.entrySet()) {
								seq.add(Expressions.set(
									schemeTo.property(recordTo, entry.getKey()),
									entry.getValue().apply(recordFrom)
								));
							}
						}))
				)
				.build(),
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
