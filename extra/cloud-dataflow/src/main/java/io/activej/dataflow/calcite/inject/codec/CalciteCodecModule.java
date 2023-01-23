package io.activej.dataflow.calcite.inject.codec;

import io.activej.codegen.DefiningClassLoader;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.RecordScheme;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

import java.lang.reflect.Type;

public class CalciteCodecModule extends AbstractModule {
	private CalciteCodecModule() {
	}

	public static CalciteCodecModule create() {
		return new CalciteCodecModule();
	}

	@Override
	protected void configure() {
		install(new OperandCodecModule());
		install(new FunctionCodecModule());
		install(new ComparatorCodecModule());
		install(new WherePredicateCodecModule());
		install(new JavaTypeCodecModule());
		install(new LeftJoinerCodecModule());
		install(new ReducerCodecModule());
		install(new RecordReducerCodecModule());
		install(new NodeCodecModule());
		install(new StreamSchemaCodecModule());
	}

	@Provides
	StreamCodec<RecordScheme> recordScheme(
			DefiningClassLoader classLoader,
			StreamCodec<Type> typeStreamCodec
	) {
		return StreamCodec.create((fieldNames, fieldTypes) -> {
					RecordScheme.Builder recordSchemeBuilder = RecordScheme.builder(classLoader);

					for (int i = 0; i < fieldNames.size(); i++) {
						String fieldName = fieldNames.get(i);
						Type fieldType = fieldTypes.get(i);

						recordSchemeBuilder.withField(fieldName, fieldType);
					}

					return recordSchemeBuilder
							.withComparatorFields(fieldNames)
							.build();
				},
				RecordScheme::getFields, StreamCodecs.ofList(StreamCodecs.ofString()),
				RecordScheme::getTypes, StreamCodecs.ofList(typeStreamCodec)
		);
	}
}
