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

package io.activej.specializer;

import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.*;
import org.objectweb.asm.commons.AnalyzerAdapter;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.tree.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.specializer.Utils.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.getMethodDescriptor;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.GeneratorAdapter.NE;
import static org.objectweb.asm.commons.Method.getMethod;

@SuppressWarnings("unchecked")
public final class Specializer {
	private static final AtomicInteger STATIC_VALUE_N = new AtomicInteger();
	private static final Map<Integer, Object> STATIC_VALUES = new ConcurrentHashMap<>();

	private final BytecodeClassLoader classLoader;
	private BiFunction<Object, Integer, String> classNaming = (instance, n) -> {
		String packageName = instance.getClass().getPackage().getName();
		return (packageName.isEmpty() ? "" : packageName + ".") + "SpecializedClass_" + n;
	};
	private Predicate<Class<?>> predicate;
	private final Map<IdentityKey<?>, Specialization> specializations = new HashMap<>();
	private Path bytecodeSaveDir;

	private Specializer(ClassLoader parent) {
		this.classLoader = new BytecodeClassLoader(parent);
	}

	private Specializer() {
		this.classLoader = new BytecodeClassLoader();
	}

	public static Specializer create(ClassLoader parent) {
		return new Specializer(parent);
	}

	public static Specializer create() {
		return new Specializer();
	}

	public Specializer withClassNaming(Function<Class<?>, String> classNaming) {
		return withClassNaming((instance, n) -> classNaming.apply(instance.getClass()) + n);
	}

	public Specializer withClassNaming(BiFunction<Object, Integer, String> classNaming) {
		this.classNaming = classNaming;
		return this;
	}

	public Specializer withPredicate(Predicate<Class<?>> predicate) {
		this.predicate = predicate;
		return this;
	}

	public Specializer withBytecodeSaveDir(Path bytecodeSaveDir) {
		this.bytecodeSaveDir = bytecodeSaveDir;
		return this;
	}

	@SuppressWarnings("PointlessBooleanExpression")
	final class Specialization {
		public static final String THIS = "$this";

		final Object instance;
		final Class<?> instanceClass;
		final Type specializedType;
		Class<?> specializedClass;
		Object specializedInstance;

		final List<Specialization> relatedSpecializations = new ArrayList<>(List.of(this));

		final Map<java.lang.reflect.Field, String> specializedFields = new LinkedHashMap<>();
		final Map<java.lang.reflect.Method, String> specializedMethods = new LinkedHashMap<>();

		Specialization(Object instance) {
			this.instance = instance;
			this.instanceClass = normalizeClass(instance.getClass());
			this.specializedType = Type.getType(
					"L" + classNaming.apply(instance, classLoader.classN.incrementAndGet()).replace('.', '/') + ";");
		}

		void scanInstance() {
			for (Class<?> clazz = instance.getClass(); clazz != Object.class; clazz = clazz.getSuperclass()) {
				for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
					specializedFields.put(field,
							field.getDeclaringClass().getSimpleName() + "$" + field.getName());
				}
			}

			for (Class<?> clazz = instance.getClass(); clazz != Object.class; clazz = clazz.getSuperclass()) {
				for (java.lang.reflect.Method javaMethod : clazz.getDeclaredMethods()) {
					if (Modifier.isStatic(javaMethod.getModifiers())) continue;
					if (Modifier.isAbstract(javaMethod.getModifiers())) continue;
					specializedMethods.put(javaMethod,
							javaMethod.getDeclaringClass().getSimpleName() + "$" + javaMethod.getName());
				}
			}

			for (Field field : specializedFields.keySet()) {
				if (!Modifier.isFinal(field.getModifiers())) continue;
				if (field.getType().isPrimitive()) continue;
				if (field.getType().isArray() || field.getType().getPackage().getName().startsWith("java.lang."))
					continue;
				field.setAccessible(true);
				Object fieldInstance;
				try {
					fieldInstance = field.get(this.instance);
				} catch (IllegalAccessException e) {
					throw new IllegalArgumentException(e);
				}
				if (fieldInstance == null) continue;
				Class<?> fieldInstanceClazz = fieldInstance.getClass();
				if (fieldInstanceClazz.isSynthetic()) continue;
				if (fieldInstanceClazz.getClassLoader() instanceof BytecodeClassLoader) continue;
				if (predicate != null && !predicate.test(fieldInstance.getClass())) continue;
				relatedSpecializations.add(ensureSpecialization(fieldInstance));
			}
		}

		public Object ensureInstance() {
			if (specializedInstance != null) return specializedInstance;
			try {
				specializedInstance = ensureClass().getDeclaredConstructor().newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return specializedInstance;
		}

		public Class<?> ensureClass() {
			if (specializedClass != null) return specializedClass;
			byte[] bytecode = defineNewClass();
			String className = specializedType.getClassName();
			classLoader.register(className, bytecode);
			try {
				specializedClass = classLoader.loadClass(className);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
			return specializedClass;
		}

		byte[] defineNewClass() {
			Set<Class<?>> interfaces = new HashSet<>();
			for (Class<?> clazz = instance.getClass(); clazz != Object.class; clazz = clazz.getSuperclass()) {
				interfaces.addAll(List.of(clazz.getInterfaces()));
			}

			ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
			cw.visit(V1_8, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
					specializedType.getInternalName(),
					null,
					Type.getInternalName(Object.class),
					interfaces.stream().map(Type::getInternalName).toArray(String[]::new));

			cw.visitField(ACC_PUBLIC | ACC_STATIC | ACC_FINAL, "$this",
					Type.getType(instanceClass).getDescriptor(), null, null);

			for (Map.Entry<java.lang.reflect.Field, String> entry : specializedFields.entrySet()) {
				java.lang.reflect.Field javaField = entry.getKey();
				String name = entry.getValue();

				cw.visitField(ACC_PUBLIC | ACC_STATIC | (javaField.getModifiers() & (ACC_FINAL | ACC_VOLATILE)), name,
						Type.getType(javaField.getType()).getDescriptor(), null, null);
			}

			{
				Method m = getMethod("void <clinit> ()");
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC | ACC_STATIC, m, null, null, cw);

				g.push(registerStaticValue(instance));
				g.invokeStatic(Type.getType(Specializer.class),
						new Method("takeStaticValue", getType(Object.class), new Type[]{getType(int.class)}));
				g.checkCast(getType(instanceClass));
				g.putStatic(specializedType, THIS, getType(instanceClass));

				for (Map.Entry<java.lang.reflect.Field, String> entry : specializedFields.entrySet()) {
					java.lang.reflect.Field javaField = entry.getKey();
					String fieldName = entry.getValue();

					javaField.setAccessible(true);
					Object fieldInstance;
					try {
						fieldInstance = javaField.get(this.instance);
					} catch (IllegalAccessException e) {
						throw new IllegalArgumentException(e);
					}
					if (fieldInstance == null) {
						g.visitInsn(ACONST_NULL);
					} else {
						g.push(registerStaticValue(fieldInstance));
						g.invokeStatic(Type.getType(Specializer.class),
								new Method("takeStaticValue", getType(Object.class), new Type[]{getType(int.class)}));
					}
					if (javaField.getType().isPrimitive()) {
						g.checkCast(getType(getBoxedType(javaField.getType())));
						g.unbox(getType(javaField.getType()));
					} else {
						g.checkCast(getType(javaField.getType()));
					}
					g.putStatic(specializedType, fieldName, getType(javaField.getType()));
				}

				g.returnValue();
				g.endMethod();
			}

			{
				Method m = getMethod("void <init> ()");
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);
				g.loadThis();
				g.invokeConstructor(getType(Object.class), m);
				g.returnValue();
				g.endMethod();
			}

			for (Map.Entry<java.lang.reflect.Method, String> entry : specializedMethods.entrySet()) {
				java.lang.reflect.Method javaMethod = entry.getKey();
				String specializedMethodName = entry.getValue();
				ClassNode classNode = ensureClassNode(javaMethod.getDeclaringClass());
				String methodDesc = getMethodDescriptor(
						getType(javaMethod.getReturnType()),
						Arrays.stream(javaMethod.getParameterTypes()).map(Type::getType).toArray(Type[]::new));
				//noinspection OptionalGetWithoutIsPresent

				transformMethod(
						classNode.methods.stream()
								.filter(methodNode -> true &&
										methodNode.name.equals(javaMethod.getName()) &&
										methodNode.desc.equals(methodDesc))
								.findFirst()
								.get(),
						new GeneratorAdapter(ACC_PUBLIC | ACC_STATIC | ACC_FINAL,
								new Method(specializedMethodName, methodDesc), null, null, cw));
			}

			Set<Method> interfaceMethods = interfaces.stream()
					.flatMap(i -> Arrays.stream(i.getMethods())
							.map(m -> new Method(
									m.getName(),
									getMethodDescriptor(
											getType(m.getReturnType()),
											Arrays.stream(m.getParameterTypes()).map(Type::getType).toArray(Type[]::new)))))
					.collect(toSet());

			for (Method method : interfaceMethods) {
				String methodImpl = lookupMethod(instance.getClass(), method);
				if (methodImpl == null) continue;
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC | ACC_FINAL, method, null, null, cw);
				for (int i = 0; i < method.getArgumentTypes().length; i++) {
					g.loadArg(i);
				}
				g.invokeStatic(specializedType, new Method(methodImpl, method.getDescriptor()));
				g.returnValue();
				g.endMethod();
			}

			cw.visitEnd();

			if (bytecodeSaveDir != null) {
				try (FileOutputStream fos = new FileOutputStream(bytecodeSaveDir.resolve(specializedType.getClassName() + ".class").toFile())) {
					fos.write(cw.toByteArray());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			return cw.toByteArray();
		}

		@SuppressWarnings({"ConstantConditions", "DuplicateBranchesInSwitch"})
		void transformMethod(MethodNode methodNode, GeneratorAdapter g) {
			AnalyzerAdapter analyzerAdapter = new AnalyzerAdapter(getType(instanceClass).getInternalName(), ACC_PUBLIC | ACC_FINAL, methodNode.name, methodNode.desc, null);

			Type[] methodParameters = new Method(methodNode.name, methodNode.desc).getArgumentTypes();

			class Remapping {
				final int slot;
				final Type type;

				Remapping(int slot, Type type) {
					this.slot = slot;
					this.type = type;
				}
			}
			List<Remapping> remappings = new ArrayList<>();

			AbstractInsnNode insn;
			for (int i = 0; i < methodNode.instructions.size(); i++, insn.accept(analyzerAdapter)) {
				insn = methodNode.instructions.get(i);

				int opcode = insn.getOpcode();

				if (insn instanceof LabelNode labelNode) {
					g.visitLabel(labelNode.getLabel());
					continue;
				}

				if (insn instanceof FrameNode) {
					continue;
				}

				if (insn instanceof LineNumberNode) {
					continue;
				}

				switch (opcode) {
					case ACONST_NULL, ICONST_M1, ICONST_0, ICONST_1, ICONST_2, ICONST_3, ICONST_4, ICONST_5, LCONST_0,
							LCONST_1, FCONST_0, FCONST_1, FCONST_2, DCONST_0, DCONST_1 -> g.visitInsn(opcode);
					case BIPUSH, SIPUSH -> g.visitIntInsn(opcode, ((IntInsnNode) insn).operand);
					case LDC -> g.visitLdcInsn(((LdcInsnNode) insn).cst);
					case ILOAD, LLOAD, FLOAD, DLOAD, ALOAD -> {
						VarInsnNode insnVar = (VarInsnNode) insn;
						if (insnVar.var == 0) {
							g.getStatic(specializedType, THIS, getType(instanceClass));
							break;
						}
						if (insnVar.var - 1 < methodParameters.length) {
							g.loadArg(insnVar.var - 1);
							break;
						}
						g.loadLocal(remappings.get(insnVar.var).slot);
					}
					case IALOAD, LALOAD, FALOAD, DALOAD, AALOAD, BALOAD, CALOAD, SALOAD -> g.visitInsn(opcode);
					case ISTORE, LSTORE, FSTORE, DSTORE, ASTORE -> {
						VarInsnNode insnVar = (VarInsnNode) insn;
						int var = insnVar.var;

						if (var - 1 < methodParameters.length) {
							g.storeArg(var - 1);
							break;
						}

						Object top = analyzerAdapter.stack.get(analyzerAdapter.stack.size() - 1);
						if (top == Opcodes.TOP) top = analyzerAdapter.stack.get(analyzerAdapter.stack.size() - 2);
						Type topType;
						if (top == Opcodes.INTEGER) topType = Type.INT_TYPE;
						else if (top == Opcodes.FLOAT) topType = Type.FLOAT_TYPE;
						else if (top == Opcodes.DOUBLE) topType = Type.DOUBLE_TYPE;
						else if (top == Opcodes.LONG) topType = Type.LONG_TYPE;
						else if (top == Opcodes.NULL) topType = getType(Object.class);
						else if (top instanceof String) topType = Type.getType(internalizeClassName((String) top));
						else throw new UnsupportedOperationException("" + top + " " + insn);

						@Nullable Remapping remapping = var < remappings.size() ? remappings.get(var) : null;
						if (remapping != null && topType.getSort() == remapping.type.getSort()) {
							g.storeLocal(remapping.slot);
						} else {
							int newLocal = g.newLocal(topType);
							while (var >= remappings.size()) {
								remappings.add(null);
							}
							remappings.set(var, new Remapping(newLocal, topType));
							g.storeLocal(newLocal);
						}
					}
					case IASTORE, LASTORE, FASTORE, DASTORE, AASTORE, BASTORE, CASTORE, SASTORE -> g.visitInsn(opcode);
					case POP, POP2, DUP, DUP_X1, DUP_X2, DUP2, DUP2_X1, DUP2_X2 -> g.visitInsn(opcode);
					case IADD, LADD, FADD, DADD, ISUB, LSUB, FSUB, DSUB, IMUL, LMUL, FMUL, DMUL, IDIV, LDIV, FDIV, DDIV,
							IREM, LREM, FREM, DREM, INEG, LNEG, FNEG, DNEG, ISHL, LSHL, ISHR, LSHR, IUSHR, LUSHR, IAND,
							LAND, IOR, LOR, IXOR, LXOR -> g.visitInsn(opcode);
					case IINC -> {
						IincInsnNode insnInc = (IincInsnNode) insn;
						int var = insnInc.var;
						if (var - 1 < methodParameters.length) {
							g.visitIincInsn(var, insnInc.incr);
							break;
						}
						g.iinc(remappings.get(insnInc.var).slot, insnInc.incr);
					}

					case I2L, I2F, I2D, L2I, L2F, L2D, F2I, F2L, F2D, D2I, D2L, D2F, I2B, I2C, I2S ->
							g.visitInsn(opcode);

					case IFEQ, IFNE, IFLT, IFGE, IFGT, IFLE, IF_ICMPEQ, IF_ICMPNE, IF_ICMPLT, IF_ICMPGE, IF_ICMPGT,
							IF_ICMPLE, IF_ACMPEQ, IF_ACMPNE, GOTO, IFNULL, IFNONNULL ->
							g.visitJumpInsn(opcode, ((JumpInsnNode) insn).label.getLabel());

					case GETSTATIC -> {
						FieldInsnNode insnField = (FieldInsnNode) insn;
						Type ownerType = getType(internalizeClassName(insnField.owner));
						doCallStatic(ownerType,
								s -> Optional.ofNullable(s.lookupField(s.instance.getClass(), insnField.name))
										.map(lookupField ->
												() -> g.getStatic(s.specializedType, lookupField, getType(insnField.desc))),
								() -> g.visitFieldInsn(GETSTATIC, insnField.owner, insnField.name, insnField.desc));
						break;
					}
					case PUTSTATIC -> {
						FieldInsnNode insnField = (FieldInsnNode) insn;
						Type ownerType = getType(internalizeClassName(insnField.owner));
						doCallStatic(ownerType,
								s -> Optional.ofNullable(s.lookupField(s.instance.getClass(), insnField.name))
										.map(lookupField ->
												() -> g.putStatic(s.specializedType, lookupField, getType(insnField.desc))),
								() -> g.visitFieldInsn(PUTSTATIC, insnField.owner, insnField.name, insnField.desc));
					}
					case GETFIELD -> {
						FieldInsnNode insnField = (FieldInsnNode) insn;
						Type ownerType = getType(internalizeClassName(insnField.owner));
						doCall(g, ownerType, new Type[]{},
								s -> Optional.ofNullable(s.lookupField(s.instance.getClass(), insnField.name))
										.map(lookupField ->
												() -> g.getStatic(s.specializedType, lookupField, getType(insnField.desc))),
								() -> g.visitFieldInsn(GETFIELD, insnField.owner, insnField.name, insnField.desc));
					}
					case PUTFIELD -> {
						FieldInsnNode insnField = (FieldInsnNode) insn;
						Type ownerType = getType(internalizeClassName(insnField.owner));
						doCall(g, ownerType, new Type[]{getType(insnField.desc)},
								s -> Optional.ofNullable(s.lookupField(s.instance.getClass(), insnField.name))
										.map(lookupField ->
												() -> g.putStatic(s.specializedType, lookupField, getType(insnField.desc))),
								() -> g.visitFieldInsn(PUTFIELD, insnField.owner, insnField.name, insnField.desc));
					}
					case INVOKESTATIC -> {
						MethodInsnNode insnMethod = (MethodInsnNode) insn;
						g.visitMethodInsn(INVOKESTATIC, insnMethod.owner, insnMethod.name, insnMethod.desc, false);
					}
					case INVOKEINTERFACE, INVOKEVIRTUAL -> {
						MethodInsnNode insnMethod = (MethodInsnNode) insn;
						Method method = new Method(insnMethod.name, insnMethod.desc);
						Type ownerType = getType(internalizeClassName(insnMethod.owner));
						doCall(g, ownerType, method.getArgumentTypes(),
								s -> Optional.ofNullable(s.lookupMethod(s.instance.getClass(), method))
										.map(lookupMethod ->
												() -> g.invokeStatic(s.specializedType, new Method(lookupMethod, method.getDescriptor()))),
								() -> {
									if (opcode == INVOKEINTERFACE) {
										g.invokeInterface(ownerType, method);
									} else if (opcode == INVOKEVIRTUAL) {
										g.invokeVirtual(ownerType, method);
									}

								});
					}
					case INVOKESPECIAL -> {
						MethodInsnNode insnMethod = (MethodInsnNode) insn;
						if (insnMethod.name.equals("<init>")) {
							g.visitMethodInsn(INVOKESPECIAL, insnMethod.owner, insnMethod.name, insnMethod.desc, false);
							break;
						}
						Method method = new Method(insnMethod.name, insnMethod.desc);

						List<Integer> paramLocals = new ArrayList<>();
						for (Type type : method.getArgumentTypes()) {
							int paramLocal = g.newLocal(type);
							paramLocals.add(paramLocal);
							g.storeLocal(paramLocal);
						}
						Collections.reverse(paramLocals);

						g.pop();
						for (int paramLocal : paramLocals) {
							g.loadLocal(paramLocal);
						}

						String name = lookupMethod(
								loadClass(classLoader, getType(internalizeClassName(insnMethod.owner))),
								method);
						g.invokeStatic(specializedType,
								new Method(
										name,
										method.getDescriptor()));
					}
					case INVOKEDYNAMIC -> {
						InvokeDynamicInsnNode insnInvokeDynamic = (InvokeDynamicInsnNode) insn;
						g.visitInvokeDynamicInsn(insnInvokeDynamic.name, insnInvokeDynamic.desc, insnInvokeDynamic.bsm, insnInvokeDynamic.bsmArgs);
					}
					case NEW, NEWARRAY, ANEWARRAY -> g.visitTypeInsn(opcode, ((TypeInsnNode) insn).desc);
					case ARRAYLENGTH -> g.visitInsn(opcode);
					case ATHROW -> g.visitInsn(opcode);
					case CHECKCAST, INSTANCEOF -> g.visitTypeInsn(opcode, ((TypeInsnNode) insn).desc);
					case MONITORENTER, MONITOREXIT -> g.visitInsn(opcode);
					case ARETURN, IRETURN, FRETURN, LRETURN, DRETURN, RETURN -> g.visitInsn(opcode);
					default -> throw new UnsupportedOperationException("" + opcode + " " + insn);
				}
			}

			for (int i = 0; i < methodNode.tryCatchBlocks.size(); i++) {
				TryCatchBlockNode tryCatchBlock = methodNode.tryCatchBlocks.get(i);
				g.visitTryCatchBlock(tryCatchBlock.start.getLabel(), tryCatchBlock.end.getLabel(), tryCatchBlock.handler.getLabel(),
						tryCatchBlock.type);
			}

			g.endMethod();
		}

		private void doCall(GeneratorAdapter g,
				Type ownerType, Type[] paramTypes,
				Function<Specialization, Optional<Runnable>> staticCallSupplier,
				Runnable defaultCall) {

			Class<?> ownerClazz = loadClass(classLoader, ownerType);

			int[] paramLocals = new int[paramTypes.length];
			for (int j = paramTypes.length - 1; j >= 0; j--) {
				Type type = paramTypes[j];
				int paramLocal = g.newLocal(type);
				paramLocals[j] = paramLocal;
				g.storeLocal(paramLocal);
			}

			Label labelExit = g.newLabel();

			for (Specialization s : relatedSpecializations) {
				if (!ownerClazz.isAssignableFrom(s.instance.getClass())) continue;
				Optional<Runnable> staticCall = staticCallSupplier.apply(s);
				if (staticCall.isEmpty()) continue;

				Label labelNext = g.newLabel();

				g.dup();
				g.getStatic(s.specializedType, THIS, getType(s.instanceClass));
				g.ifCmp(getType(Object.class), NE, labelNext);

				g.pop();

				for (int paramLocal : paramLocals) {
					g.loadLocal(paramLocal);
				}
				staticCall.get().run();
				g.goTo(labelExit);

				g.mark(labelNext);
			}

			g.checkCast(ownerType);
			for (int paramLocal : paramLocals) {
				g.loadLocal(paramLocal);
			}
			defaultCall.run();

			g.mark(labelExit);
		}

		private void doCallStatic(Type ownerType,
				Function<Specialization, Optional<Runnable>> staticCallSupplier,
				Runnable defaultCall) {

			Class<?> ownerClazz = loadClass(classLoader, ownerType);

			for (Specialization s : relatedSpecializations) {
				if (!ownerClazz.isAssignableFrom(s.instance.getClass())) continue;
				Optional<Runnable> staticCall = staticCallSupplier.apply(s);
				if (staticCall.isEmpty()) continue;

				staticCall.get().run();
				return;
			}

			defaultCall.run();
		}

		@Nullable String lookupField(Class<?> owner, String field) {
			java.lang.reflect.Field result = null;
			for (java.lang.reflect.Field originalField : specializedFields.keySet()) {
				if (true &&
						Objects.equals(originalField.getName(), field) &&
						originalField.getDeclaringClass().isAssignableFrom(owner) &&
						(result == null ||
								result.getDeclaringClass().isAssignableFrom(originalField.getDeclaringClass()))) {
					result = originalField;
				}
			}
			return specializedFields.get(result);
		}

		@Nullable String lookupMethod(Class<?> owner, Method method) {
			java.lang.reflect.Method result = null;
			for (java.lang.reflect.Method originalMethod : specializedMethods.keySet()) {
				if (true &&
						Objects.equals(originalMethod.getName(), method.getName()) &&
						Objects.equals(
								Arrays.stream(originalMethod.getParameters()).map(p -> getType(p.getType())).collect(toList()),
								List.of(method.getArgumentTypes())) &&
						originalMethod.getDeclaringClass().isAssignableFrom(owner) &&
						(result == null ||
								result.getDeclaringClass().isAssignableFrom(originalMethod.getDeclaringClass()))) {
					result = originalMethod;
				}
			}
			return specializedMethods.get(result);
		}

		private ClassNode ensureClassNode(Class<?> clazz) {
			ClassNode classNode = new ClassNode();
			ClassReader cr;
			try {
				ClassLoader classLoader = clazz.getClassLoader();
				String pathToClass = clazz.getName().replace('.', '/') + ".class";
				InputStream classInputStream = classLoader.getResourceAsStream(pathToClass);
				//noinspection ConstantConditions - null is allowed
				cr = new ClassReader(classInputStream);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
			cr.accept(classNode, ClassReader.SKIP_DEBUG | ClassReader.EXPAND_FRAMES);
			return classNode;
		}

	}

	private static synchronized int registerStaticValue(Object value) {
		int idx = STATIC_VALUE_N.incrementAndGet();
		STATIC_VALUES.put(idx, value);
		return idx;
	}

	@SuppressWarnings("unused")
	public static synchronized Object takeStaticValue(int idx) {
		return STATIC_VALUES.remove(idx);
	}

	public <T> T specialize(T instance) {
		if (instance.getClass().getClassLoader() instanceof BytecodeClassLoader) return instance;
		if (predicate != null && !predicate.test(instance.getClass())) return instance;
		ensureAccessibility(instance.getClass());
		Specialization specialization = ensureSpecialization(instance);
		for (Specialization s : specializations.values()) {
			s.ensureClass();
		}
		return (T) specialization.ensureInstance();
	}

	private static void ensureAccessibility(Class<?> clazz) {
		for (Field field : clazz.getDeclaredFields()) {
			Class<?> fieldType = field.getType();
			if (fieldType.isAnonymousClass()) {
				throw new IllegalArgumentException("Field type cannot be anonymous class: " + field);
			}
			if (!Modifier.isPublic(fieldType.getModifiers())) {
				throw new IllegalArgumentException("Field type is not accessible: " + fieldType);
			}
		}
	}

	private <T> Specialization ensureSpecialization(T instance) {
		IdentityKey<Object> key = new IdentityKey<>(instance);
		Specialization specialization = specializations.get(key);
		if (specialization == null) {
			specialization = new Specialization(instance);
			specializations.put(key, specialization);
			specialization.scanInstance();
		}
		return specialization;
	}

	public boolean isSpecialized(Object instance) {
		return specializations.containsKey(new IdentityKey<>(instance));
	}

	public BytecodeClassLoader getClassLoader() {
		return classLoader;
	}
}
