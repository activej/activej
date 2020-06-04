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

package io.activej.codegen.expression;

import io.activej.codegen.Context;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.Iterator;
import java.util.function.Function;

import static org.objectweb.asm.Type.BOOLEAN_TYPE;
import static org.objectweb.asm.Type.getType;

public abstract class AbstractExpressionMapForEach implements Expression {
	protected final Expression collection;
	protected final Function<Expression, Expression> forKey;
	protected final Function<Expression, Expression> forValue;
	protected final Class<?> entryClazz;

	protected AbstractExpressionMapForEach(Expression collection, Function<Expression, Expression> forKey, Function<Expression, Expression> forValue, Class<?> entryClazz) {
		this.collection = collection;
		this.forKey = forKey;
		this.forValue = forValue;
		this.entryClazz = entryClazz;
	}

	protected abstract Expression getEntries();

	protected abstract Expression getKey(VarLocal entry);

	protected abstract Expression getValue(VarLocal entry);

	@Override
	public final Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		ctx.invoke(getEntries(), "iterator");
		VarLocal iterator = ctx.newLocal(getType(Iterator.class));
		iterator.store(ctx);

		g.mark(labelLoop);

		ctx.invoke(iterator, "hasNext");
		g.push(false);
		g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.EQ, labelExit);

		Type entryType = getType(entryClazz);
		ctx.cast(ctx.invoke(iterator, "next"), entryType);

		VarLocal entry = ctx.newLocal(entryType);
		entry.store(ctx);

		Type forKeyType = forKey.apply(getKey(entry)).load(ctx);
		if (forKeyType.getSize() == 1)
			g.pop();
		if (forKeyType.getSize() == 2)
			g.pop2();

		Type forValueType = forValue.apply(getValue(entry)).load(ctx);
		if (forValueType.getSize() == 1)
			g.pop();
		if (forValueType.getSize() == 2)
			g.pop2();

		g.goTo(labelLoop);
		g.mark(labelExit);
		return Type.VOID_TYPE;
	}
}
