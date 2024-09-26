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

package io.activej.jmx;

import io.activej.common.ref.Ref;
import io.activej.common.reflection.ReflectionUtils;
import io.activej.jmx.api.ConcurrentJmxBeanAdapter;
import io.activej.jmx.api.JmxBeanAdapter;
import io.activej.jmx.api.JmxBeanAdapterWithRefresh;
import io.activej.jmx.api.JmxRefreshable;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.api.attribute.JmxParameter;
import io.activej.jmx.api.attribute.JmxReducer;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerDistinct;
import io.activej.jmx.stats.JmxRefreshableStats;
import io.activej.jmx.stats.JmxStats;
import io.activej.types.Types;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.activej.common.Checks.*;
import static io.activej.common.Utils.first;
import static io.activej.common.Utils.nonNullElse;
import static io.activej.common.reflection.ReflectionUtils.*;
import static io.activej.jmx.Utils.findAdapterClass;
import static io.activej.jmx.stats.StatsUtils.isJmxStats;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("rawtypes")
public final class DynamicMBeanFactory {
	private static final Logger logger = LoggerFactory.getLogger(DynamicMBeanFactory.class);

	// refreshing jmx
	public static final Duration DEFAULT_REFRESH_PERIOD_IN_SECONDS = Duration.ofSeconds(1);
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = 500;
	private int maxJmxRefreshesPerOneCycle;
	private Duration specifiedRefreshPeriod;
	private final Map<Class<? extends JmxBeanAdapter>, JmxBeanAdapter> adapters = new HashMap<>();

	private static final JmxReducer<?> DEFAULT_REDUCER = new JmxReducerDistinct();

	// JmxStats creator methods
	private static final String CREATE = "create";
	private static final String CREATE_ACCUMULATOR = "createAccumulator";

	private static final DynamicMBeanFactory INSTANCE_WITH_DEFAULT_REFRESH_PERIOD = new DynamicMBeanFactory(DEFAULT_REFRESH_PERIOD_IN_SECONDS, MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT);

	private DynamicMBeanFactory(Duration refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		this.specifiedRefreshPeriod = refreshPeriod;
		this.maxJmxRefreshesPerOneCycle = maxJmxRefreshesPerOneCycle;
	}

	public static DynamicMBeanFactory create() {
		return INSTANCE_WITH_DEFAULT_REFRESH_PERIOD;
	}

	public static DynamicMBeanFactory create(Duration refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		return new DynamicMBeanFactory(refreshPeriod, maxJmxRefreshesPerOneCycle);
	}

	// region exportable stats for JmxRegistry
	public Duration getSpecifiedRefreshPeriod() {
		return specifiedRefreshPeriod;
	}

	public void setRefreshPeriod(Duration refreshPeriod) {
		this.specifiedRefreshPeriod = refreshPeriod;
		updateAdaptersRefreshParameters();
	}

	public int getMaxJmxRefreshesPerOneCycle() {
		return maxJmxRefreshesPerOneCycle;
	}

	public void setMaxJmxRefreshesPerOneCycle(int maxJmxRefreshesPerOneCycle) {
		this.maxJmxRefreshesPerOneCycle = maxJmxRefreshesPerOneCycle;
		updateAdaptersRefreshParameters();
	}

	public String[] getRefreshStats() {
		return adapters.values().stream()
			.filter(adapter -> adapter instanceof JmxBeanAdapterWithRefresh)
			.map(JmxBeanAdapterWithRefresh.class::cast)
			.map(JmxBeanAdapterWithRefresh::getRefreshStats)
			.flatMap(Collection::stream)
			.toArray(String[]::new);
	}
	// endregion

	/**
	 * Creates Jmx MBean for beans with operations and attributes.
	 */
	public DynamicMBean createDynamicMBean(List<?> beans, JmxBeanSettings setting, boolean enableRefresh) {
		checkArgument(!beans.isEmpty(), "List of beans should not be empty");
		checkArgument(beans.stream().noneMatch(Objects::isNull), "Bean can not be null");
		checkArgument(beans.stream().map(Object::getClass).collect(toSet()).size() == 1, "Beans should be of the same type");

		Class<?> beanClass = beans.get(0).getClass();

		JmxBeanAdapter adapter = ensureAdapter(beanClass);
		if (adapter.getClass().equals(ConcurrentJmxBeanAdapter.class)) {
			checkArgument(beans.size() == 1, "ConcurrentJmxBeans cannot be used in pool");
		}

		AttributeNodeForPojo rootNode = createAttributesTree(beanClass, setting.getCustomTypes());
		rootNode.hideNullPojos(beans);

		for (String included : setting.getIncludedOptionals()) {
			rootNode.setVisible(included);
		}

		// TODO(vmykhalko): check in JmxRegistry that modifiers are applied only once in case of workers and pool registration
		for (String attrName : setting.getModifiers().keySet()) {
			AttributeModifier<?> modifier = setting.getModifiers().get(attrName);
			try {
				rootNode.applyModifier(attrName, modifier, beans);
			} catch (ClassCastException e) {
				throw new IllegalArgumentException(format("Cannot apply modifier \"%s\" for attribute \"%s\": %s",
					modifier.getClass().getName(), attrName, e));
			}
		}

		MBeanInfo mBeanInfo = createMBeanInfo(rootNode, beanClass, setting.getCustomTypes());
		Map<OperationKey, Invokable> opkeyToInvokable = fetchOpkeyToInvokable(beanClass, setting.getCustomTypes());

		DynamicMBeanAggregator mbean = new DynamicMBeanAggregator(mBeanInfo, adapter, beans, rootNode, opkeyToInvokable);

		// TODO(vmykhalko): maybe try to get all attributes and log warn message in case of exception? (to prevent potential errors during viewing jmx stats using jconsole)
//		tryGetAllAttributes(mbean);

		if (enableRefresh && adapter instanceof JmxBeanAdapterWithRefresh) {
			for (Object bean : beans) {
				((JmxBeanAdapterWithRefresh) adapter).registerRefreshableBean(bean, rootNode.getAllRefreshables(bean));
			}
		}
		return mbean;
	}

	JmxBeanAdapter ensureAdapter(Class<?> beanClass) {
		Class<? extends JmxBeanAdapter> adapterClass = findAdapterClass(beanClass)
			.orElseThrow(() -> new NoSuchElementException("Class or its superclass or any of implemented interfaces should be annotated with @JmxBean annotation"));
		return adapters.computeIfAbsent(adapterClass, $ -> {
			try {
				JmxBeanAdapter jmxBeanAdapter = adapterClass.getDeclaredConstructor().newInstance();
				if (jmxBeanAdapter instanceof JmxBeanAdapterWithRefresh) {
					((JmxBeanAdapterWithRefresh) jmxBeanAdapter).setRefreshParameters(specifiedRefreshPeriod, maxJmxRefreshesPerOneCycle);
				}
				return jmxBeanAdapter;
			} catch (ReflectiveOperationException e) {
				throw new RuntimeException(e);
			}
		});
	}

	// region building tree of AttributeNodes
	private List<AttributeNode> createNodesFor(
		Class<?> clazz, Class<?> beanClass, String[] includedOptionalAttrs, @Nullable Method getter,
		Map<Type, JmxCustomTypeAdapter<?>> customTypes
	) {
		Set<String> includedOptionals = new HashSet<>(List.of(includedOptionalAttrs));
		List<AttributeDescriptor> attrDescriptors = fetchAttributeDescriptors(clazz, customTypes);
		List<AttributeNode> attrNodes = new ArrayList<>();
		for (AttributeDescriptor descriptor : attrDescriptors) {
			checkNotNull(descriptor.getter(), "@JmxAttribute \"%s\" does not have getter", descriptor.name());

			String attrName;
			Method attrGetter = descriptor.getter();
			JmxAttribute attrAnnotation = attrGetter.getAnnotation(JmxAttribute.class);
			String attrAnnotationName = attrAnnotation.name();
			if (attrAnnotationName.equals(JmxAttribute.USE_GETTER_NAME)) {
				attrName = extractFieldNameFromGetter(attrGetter);
			} else {
				attrName = attrAnnotationName;
			}
			checkArgument(!attrName.contains("_"), "@JmxAttribute with name \"%s\" contains underscores", attrName);

			Type type = attrGetter.getGenericReturnType();
			Class<?> rawType = Types.getRawType(type);

			String attrDescription = null;
			if (!attrAnnotation.description().equals(JmxAttribute.NO_DESCRIPTION)) {
				attrDescription = attrAnnotation.description();
			} else if (isEnumType(rawType)) {
				attrDescription = "Possible enum values: " + Arrays.toString(rawType.getEnumConstants());
			}

			boolean included = !attrAnnotation.optional() || includedOptionals.contains(attrName);
			includedOptionals.remove(attrName);

			Method attrSetter = descriptor.setter();
			AttributeNode attrNode = createAttributeNodeFor(attrName, attrDescription, type, included,
				attrAnnotation, null, attrGetter, attrSetter, beanClass,
				customTypes);
			attrNodes.add(attrNode);
		}

		if (!includedOptionals.isEmpty()) {
			assert getter != null; // in this case getter cannot be null
			throw new RuntimeException(format(
				"Error in \"extraSubAttributes\" parameter in @JmxAnnotation on %s.%s(). There is no field \"%s\" in %s.",
				getter.getDeclaringClass().getName(), getter.getName(),
				first(includedOptionals), getter.getReturnType().getName()));
		}

		return attrNodes;
	}

	private List<AttributeDescriptor> fetchAttributeDescriptors(Class<?> clazz, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Map<String, AttributeDescriptor> nameToAttr = new HashMap<>();
		for (Method method : getAllMethods(clazz)) {
			if (method.isAnnotationPresent(JmxAttribute.class)) {
				validateJmxMethod(method, JmxAttribute.class);
				if (isGetter(method)) {
					processGetter(nameToAttr, method);
				} else if (isSetter(method)) {
					processSetter(nameToAttr, method, customTypes);
				} else {
					throw new RuntimeException(format(
						"Method \"%s\" of class \"%s\" is annotated with @JmxAnnotation but is neither getter nor setter",
						method.getName(), method.getDeclaringClass().getName())
					);
				}
			}
		}
		return new ArrayList<>(nameToAttr.values());
	}

	private static void validateJmxMethod(Method method, Class<? extends Annotation> annotationClass) {
		if (!isPublic(method)) {
			throw new IllegalStateException(format("A method \"%s\" in class '%s' annotated with @%s should be declared public",
				method.getName(), method.getDeclaringClass().getName(), annotationClass.getSimpleName()));
		}
		if (!isPublic(method.getDeclaringClass())) {
			throw new IllegalStateException(format("A class '%s' containing methods annotated with @%s should be declared public",
				method.getDeclaringClass().getName(), annotationClass.getSimpleName()));
		}
	}

	private static void processGetter(Map<String, AttributeDescriptor> nameToAttr, Method getter) {
		String name = extractFieldNameFromGetter(getter);
		Type attrType = getter.getReturnType();
		if (nameToAttr.containsKey(name)) {
			AttributeDescriptor previousDescriptor = nameToAttr.get(name);

			checkArgument(previousDescriptor.getter() == null,
				"More than one getter with name \"%s\"", getter.getName());
			checkArgument(previousDescriptor.type().equals(attrType),
				"Getter with name \"%s\" has different type than appropriate setter", getter.getName());

			nameToAttr.put(name, new AttributeDescriptor(name, attrType, getter, previousDescriptor.setter()));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, getter, null));
		}
	}

	private void processSetter(Map<String, AttributeDescriptor> nameToAttr, Method setter, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Class<?> attrType = setter.getParameterTypes()[0];
		checkArgument(ReflectionUtils.isSimpleType(attrType) || isStringWrappedType(customTypes, attrType),
			"Setters are allowed only on attributes of simple, custom or Enum types. But setter \"%s\" is for neither of the above types",
			setter.getName());

		String name = extractFieldNameFromSetter(setter);

		if (nameToAttr.containsKey(name)) {
			AttributeDescriptor previousDescriptor = nameToAttr.get(name);

			checkArgument(previousDescriptor.setter() == null,
				"More than one setter with name \"%s\"", setter.getName());
			checkArgument(previousDescriptor.type().equals(attrType),
				"Setter with name \"%s\" has different type than appropriate getter", setter.getName());

			nameToAttr.put(name, new AttributeDescriptor(
				name, attrType, previousDescriptor.getter(), setter));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, null, setter));
		}
	}

	private void updateAdaptersRefreshParameters() {
		for (JmxBeanAdapter adapter : adapters.values()) {
			if (adapter instanceof JmxBeanAdapterWithRefresh) {
				((JmxBeanAdapterWithRefresh) adapter).setRefreshParameters(specifiedRefreshPeriod, maxJmxRefreshesPerOneCycle);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private AttributeNode createAttributeNodeFor(
		String attrName, @Nullable String attrDescription, Type attrType, boolean included,
		@Nullable JmxAttribute attrAnnotation, @Nullable JmxReducer<?> reducer, @Nullable Method getter,
		@Nullable Method setter, Class<?> beanClass, Map<Type, JmxCustomTypeAdapter<?>> customTypes
	) {
		if (attrType instanceof Class) {
			ValueFetcher defaultFetcher = createAppropriateFetcher(getter);
			// 5 cases: custom-type, simple-type, JmxRefreshableStats, POJO, enum
			Class<? extends JmxStats> returnClass = (Class<? extends JmxStats>) attrType;

			if (customTypes.containsKey(attrType)) {
				reducer = reducer == null ? fetchReducerFrom(getter) : reducer;

				JmxCustomTypeAdapter<?> customTypeAdapter = customTypes.get(attrType);
				return AttributeNodeForType.createCustom(attrName, attrDescription, defaultFetcher,
					included, setter,
					(Function<Object, String>) customTypeAdapter.to,
					(Function<String, Object>) customTypeAdapter.from,
					(JmxReducer<Object>) reducer
				);

			} else if (ReflectionUtils.isSimpleType(returnClass)) {
				reducer = reducer == null ? fetchReducerFrom(getter) : reducer;

				return AttributeNodeForType.createSimple(
					attrName, attrDescription, defaultFetcher, included, setter, returnClass, reducer
				);

			} else if (isThrowable(returnClass)) {
				return new AttributeNodeForThrowable(attrName, attrDescription, included, defaultFetcher);

			} else if (returnClass.isArray()) {
				Class<?> elementType = returnClass.getComponentType();
				checkNotNull(getter, "Arrays can be used only directly in POJO, JmxRefreshableStats or JmxMBeans");
				ValueFetcher fetcher = new ValueFetcherFromGetterArrayAdapter(getter);
				return createListAttributeNodeFor(attrName, attrDescription, included, fetcher, elementType, beanClass, customTypes);

			} else if (isJmxStats(returnClass)) {
				// JmxRefreshableStats case

				checkJmxStatsAreValid(returnClass, beanClass, getter);

				String[] extraSubAttributes =
					attrAnnotation != null ? attrAnnotation.extraSubAttributes() : new String[0];
				List<AttributeNode> subNodes =
					createNodesFor(returnClass, beanClass, extraSubAttributes, getter, customTypes);

				if (subNodes.isEmpty()) {
					throw new IllegalArgumentException(format(
						"JmxRefreshableStats of type \"%s\" does not have JmxAttributes",
						returnClass.getName()));
				}

				return new AttributeNodeForPojo(attrName, attrDescription, included, defaultFetcher,
					createReducerForJmxStats(returnClass), subNodes);

			} else if (isEnumType(returnClass)) {
				reducer = reducer == null ? fetchReducerFrom(getter) : reducer;

				return AttributeNodeForType.createCustom(attrName, attrDescription, defaultFetcher,
					included, setter,
					anEnum -> ((Enum<?>) anEnum).name(),
					name -> parseEnum((Class<? extends Enum>) returnClass, name),
					(JmxReducer<Object>) reducer
				);

			} else {
				String[] extraSubAttributes =
					attrAnnotation != null ? attrAnnotation.extraSubAttributes() : new String[0];
				List<AttributeNode> subNodes =
					createNodesFor(returnClass, beanClass, extraSubAttributes, getter, customTypes);

				if (subNodes.isEmpty()) {
					throw new IllegalArgumentException(format("Unrecognized type of Jmx attribute: %s", attrType.getTypeName()));
				} else {
					// POJO case

					reducer = reducer == null ? fetchReducerFrom(getter) : reducer;

					return new AttributeNodeForPojo(
						attrName, attrDescription, included, defaultFetcher, reducer == DEFAULT_REDUCER ? null : reducer, subNodes);
				}
			}
		} else if (attrType instanceof ParameterizedType) {
			return createNodeForParametrizedType(
				attrName, attrDescription, (ParameterizedType) attrType, included, getter, setter, beanClass, customTypes);
		} else {
			throw new IllegalArgumentException(format("Unrecognized type of Jmx attribute: %s", attrType.getTypeName()));
		}
	}

	@SuppressWarnings("unchecked")
	private static JmxReducer<?> createReducerForJmxStats(Class<? extends JmxStats> jmxStatsClass) {
		return (JmxReducer<Object>) sources -> {
			JmxStats accumulator = createJmxAccumulator(jmxStatsClass);
			for (Object pojo : sources) {
				JmxStats jmxStats = (JmxStats) pojo;
				if (jmxStats != null) {
					accumulator.add(jmxStats);
				}
			}
			return accumulator;
		};
	}

	private static JmxStats createJmxAccumulator(Class<? extends JmxStats> jmxStatsClass) {
		JmxStats jmxStats = ReflectionUtils.tryToCreateInstanceWithFactoryMethods(jmxStatsClass, CREATE_ACCUMULATOR, CREATE);
		if (jmxStats == null) {
			throw new RuntimeException(format("Cannot create JmxStats accumulator instance: %s", jmxStatsClass.getName()));
		}
		return jmxStats;
	}

	private static JmxReducer<?> fetchReducerFrom(@Nullable Method method) {
		if (method == null) {
			return DEFAULT_REDUCER;
		}
		Class<? extends JmxReducer> reducerClass = null;
		JmxAttribute attrAnnotation = method.getAnnotation(JmxAttribute.class);
		if (attrAnnotation != null) reducerClass = attrAnnotation.reducer();
		JmxOperation opAnnotation = method.getAnnotation(JmxOperation.class);
		if (opAnnotation != null) reducerClass = opAnnotation.reducer();

		if (reducerClass == null || reducerClass == DEFAULT_REDUCER.getClass()) {
			return DEFAULT_REDUCER;
		}
		try {
			return reducerClass.getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void checkJmxStatsAreValid(Class<?> returnClass, Class<?> beanClass, @Nullable Method getter) {
		if (JmxRefreshableStats.class.isAssignableFrom(returnClass) &&
			findAdapterClass(beanClass).filter(JmxBeanAdapterWithRefresh.class::isAssignableFrom).isEmpty()
		) {
			logger.warn(
				"JmxRefreshableStats won't be refreshed when Bean adapter does not implement JmxBeanAdapterWithRefresh. MBean class: {}",
				beanClass.getName());
		}

		if (returnClass.isInterface()) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}

		if (Modifier.isAbstract(returnClass.getModifiers())) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}

		if (!canBeCreated(returnClass, CREATE_ACCUMULATOR, CREATE)) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}
	}

	private static String createErrorMessageForInvalidJmxStatsAttribute(@Nullable Method getter) {
		String msg =
			"Return type of JmxStats attribute must be a concrete class that implements" +
			" JmxStats interface and contains" +
			" static factory \"" + CREATE_ACCUMULATOR + "()\" method or" +
			" static factory \"" + CREATE + "()\" method or" +
			" public no-arg constructor";

		if (getter != null) {
			msg += format(". Error at %s.%s()", getter.getDeclaringClass().getName(), getter.getName());
		}
		return msg;
	}

	private AttributeNode createNodeForParametrizedType(
		String attrName, @Nullable String attrDescription, ParameterizedType pType, boolean included,
		@Nullable Method getter, @Nullable Method setter, Class<?> beanClass,
		Map<Type, JmxCustomTypeAdapter<?>> customTypes
	) {
		ValueFetcher fetcher = createAppropriateFetcher(getter);
		Class<?> rawType = (Class<?>) pType.getRawType();

		if (rawType == List.class) {
			Type listElementType = pType.getActualTypeArguments()[0];
			return createListAttributeNodeFor(
				attrName, attrDescription, included, fetcher, listElementType, beanClass, customTypes);
		} else if (rawType == Map.class) {
			Type valueType = pType.getActualTypeArguments()[1];
			JmxReducer<?> reducer = fetchReducerFrom(getter);
			return createMapAttributeNodeFor(attrName, attrDescription, included, fetcher, reducer, valueType, beanClass, customTypes);
		} else if (customTypes.containsKey(rawType)) {
			JmxReducer<?> reducer = fetchReducerFrom(getter);
			return createConverterAttributeNodeFor(attrName, attrDescription, pType, included, fetcher, setter, customTypes, reducer);
		} else {
			throw new IllegalArgumentException(format("There is no support for generic class %s", pType.getTypeName()));
		}
	}

	@SuppressWarnings("unchecked")
	private AttributeNodeForType createConverterAttributeNodeFor(
		String attrName, @Nullable String attrDescription, ParameterizedType type, boolean included,
		ValueFetcher fetcher, @Nullable Method setter, Map<Type, JmxCustomTypeAdapter<?>> customTypes,
		JmxReducer<?> reducer
	) {
		Type[] actualTypes = type.getActualTypeArguments();
		for (Type genericType : actualTypes) {
			if (!customTypes.containsKey(genericType)) {
				throw new IllegalArgumentException(format("There is no support for generic type %s", type.getTypeName()));
			}
		}
		JmxCustomTypeAdapter<?> t = customTypes.get(type.getRawType());
		return AttributeNodeForType.createCustom(attrName, attrDescription, fetcher, included, setter,
			(Function<Object, String>) t.to,
			(Function<String, Object>) t.from,
			(JmxReducer<Object>) reducer
		);
	}

	private AttributeNodeForList createListAttributeNodeFor(
		String attrName, @Nullable String attrDescription, boolean included, ValueFetcher fetcher, Type listElementType,
		Class<?> beanClass, Map<Type, JmxCustomTypeAdapter<?>> customTypes
	) {
		if (listElementType instanceof Class<?> listElementClass) {
			boolean isListOfJmxRefreshable = JmxRefreshable.class.isAssignableFrom(listElementClass);
			return new AttributeNodeForList(
				attrName,
				attrDescription,
				included,
				fetcher,
				createAttributeNodeFor("", attrDescription, listElementType, true, null, null, null, null, beanClass, customTypes),
				isListOfJmxRefreshable
			);
		} else if (listElementType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) listElementType).getRawType()).getSimpleName();
			return new AttributeNodeForList(
				attrName,
				attrDescription,
				included,
				fetcher,
				createNodeForParametrizedType(
					typeName, attrDescription, (ParameterizedType) listElementType, true, null, null, beanClass,
					customTypes
				),
				false
			);
		} else {
			throw new IllegalArgumentException(format("Can't create list attribute node for List<%s>", listElementType.getTypeName()));
		}
	}

	private AttributeNodeForMap createMapAttributeNodeFor(
		String attrName, @Nullable String attrDescription, boolean included, ValueFetcher fetcher,
		JmxReducer<?> reducer, Type valueType, Class<?> beanClass, Map<Type, JmxCustomTypeAdapter<?>> customTypes
	) {
		boolean isMapOfJmxRefreshable = false;
		AttributeNode node;
		if (valueType instanceof Class<?> valueClass) {
			isMapOfJmxRefreshable = JmxRefreshable.class.isAssignableFrom(valueClass);
			node = createAttributeNodeFor("", attrDescription, valueType, true, null, reducer, null, null, beanClass, customTypes);
		} else if (valueType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) valueType).getRawType()).getSimpleName();
			node = createNodeForParametrizedType(typeName, attrDescription, (ParameterizedType) valueType, true, null, null, beanClass,
				customTypes);
		} else {
			throw new IllegalArgumentException(format("Can't create map attribute node for %s", valueType.getTypeName()));
		}
		return new AttributeNodeForMap(attrName, attrDescription, included, fetcher, node, isMapOfJmxRefreshable);
	}

	private static ValueFetcher createAppropriateFetcher(@Nullable Method getter) {
		return getter != null ? new ValueFetcherFromGetter(getter) : new ValueFetcherDirect();
	}
	// endregion

	/**
	 * Creates attribute tree of Jmx attributes for clazz.
	 */
	private AttributeNodeForPojo createAttributesTree(Class<?> clazz, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		List<AttributeNode> subNodes = createNodesFor(clazz, clazz, new String[0], null, customTypes);
		return new AttributeNodeForPojo("", null, true, new ValueFetcherDirect(), null, subNodes);
	}

	// region creating jmx metadata - MBeanInfo
	private static MBeanInfo createMBeanInfo(AttributeNodeForPojo rootNode, Class<?> beanClass, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		String beanName = "";
		String beanDescription = "";
		MBeanAttributeInfo[] attributes = rootNode != null ?
			fetchAttributesInfo(rootNode) :
			new MBeanAttributeInfo[0];
		MBeanOperationInfo[] operations = fetchOperationsInfo(beanClass, customTypes);
		return new MBeanInfo(
			beanName,
			beanDescription,
			attributes,
			null,  // constructors
			operations,
			null); //notifications
	}

	private static MBeanAttributeInfo[] fetchAttributesInfo(AttributeNodeForPojo rootNode) {
		Set<String> visibleAttrs = rootNode.getVisibleAttributes();
		Map<String, OpenType<?>> nameToType = rootNode.getOpenTypes();
		Map<String, Map<String, String>> nameToDescriptions = rootNode.getDescriptions();
		List<MBeanAttributeInfo> attrsInfo = new ArrayList<>();
		for (String attrName : visibleAttrs) {
			String description = createDescription(attrName, nameToDescriptions.get(attrName));
			OpenType<?> attrType = nameToType.get(attrName);
			boolean writable = rootNode.isSettable(attrName);
			boolean isIs = attrType.equals(SimpleType.BOOLEAN);
			attrsInfo.add(new MBeanAttributeInfo(attrName, attrType.getClassName(), description, true, writable, isIs));
		}

		return attrsInfo.toArray(new MBeanAttributeInfo[0]);
	}

	private static String createDescription(String name, Map<String, String> groupDescriptions) {
		if (groupDescriptions.isEmpty()) {
			return name;
		}

		if (!name.contains("_")) {
			assert groupDescriptions.size() == 1;
			return first(groupDescriptions.values());
		}

		return groupDescriptions.entrySet().stream()
			.map(entry -> String.format("\"%s\": %s", entry.getKey(), entry.getValue()))
			.collect(joining("  |  "));
	}

	private static MBeanOperationInfo[] fetchOperationsInfo(Class<?> beanClass, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		List<MBeanOperationInfo> operations = new ArrayList<>();
		List<Method> methods = getAllMethods(beanClass);
		for (Method method : methods) {
			if (!method.isAnnotationPresent(JmxOperation.class)) continue;

			validateJmxMethod(method, JmxOperation.class);
			JmxOperation annotation = method.getAnnotation(JmxOperation.class);
			String opName = annotation.name();
			if (opName.isEmpty()) {
				opName = method.getName();
			}

			Annotation[][] parameterAnnotations = method.getParameterAnnotations();
			Parameter[] parameters = method.getParameters();
			MBeanParameterInfo[] parameterInfos = new MBeanParameterInfo[parameters.length];
			for (int i = 0; i < parameters.length; i++) {
				Parameter parameter = parameters[i];
				Class<?> parameterType = parameter.getType();

				String defaultName = parameter.isNamePresent() ?
					parameter.getName() :
					parameterType.getSimpleName();

				if (isStringWrappedType(customTypes, parameterType)) {
					parameterType = String.class;
				}

				String description = "";
				if (isEnumType(parameterType)) {
					description = "Possible enum values: " + Arrays.toString(parameter.getType().getEnumConstants());
				}

				parameterInfos[i] = new MBeanParameterInfo(
					Arrays.stream(parameterAnnotations[i])
						.filter(a -> a.annotationType() == JmxParameter.class)
						.map(JmxParameter.class::cast)
						.map(JmxParameter::value)
						.findFirst()
						.orElse(defaultName),
					parameterType.getName(),
					description);
			}

			Class<?> returnType = method.getReturnType();

			String description = annotation.description();
			String returnTypeName;
			if (isStringWrappedType(customTypes, returnType)) {
				returnTypeName = String.class.getName();
				if (description.isEmpty()) {
					description = returnType.getName();
				}
			} else {
				returnTypeName = returnType.getName();
			}

			MBeanOperationInfo operationInfo = new MBeanOperationInfo(
				opName, description, parameterInfos, returnTypeName, MBeanOperationInfo.ACTION);
			operations.add(operationInfo);
		}

		return operations.toArray(new MBeanOperationInfo[0]);
	}

	private static boolean isStringWrappedType(Map<Type, JmxCustomTypeAdapter<?>> customTypes, Class<?> parameterType) {
		return isEnumType(parameterType) || customTypes.containsKey(parameterType);
	}

	private static boolean isEnumType(Class<?> parameterType) {
		return Enum.class.isAssignableFrom(parameterType);
	}
	// endregion

	// region jmx operations fetching
	@SuppressWarnings("unchecked")
	private Map<OperationKey, Invokable> fetchOpkeyToInvokable(Class<?> beanClass, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Map<OperationKey, Invokable> opkeyToInvokable = new HashMap<>();
		List<Method> methods = getAllMethods(beanClass);
		for (Method method : methods) {
			if (!method.isAnnotationPresent(JmxOperation.class)) continue;

			JmxOperation annotation = method.getAnnotation(JmxOperation.class);
			String opName = annotation.name();
			if (opName.isEmpty()) {
				opName = method.getName();
			}
			Class<?>[] paramTypes = method.getParameterTypes();
			Annotation[][] paramAnnotations = method.getParameterAnnotations();

			assert paramAnnotations.length == paramTypes.length;

			String[] paramTypesNames = new String[paramTypes.length];
			for (int i = 0; i < paramTypes.length; i++) {
				paramTypesNames[i] = paramTypes[i].getName();
			}

			Invokable invokable;
			if (isGetter(method)) {
				String name = extractFieldNameFromGetter(method);
				AttributeNode node = createAttributeNodeFor(name, null, method.getGenericReturnType(), true, null, null, method, null, beanClass, customTypes);
				invokable = Invokable.ofAttributeNode(node);
			} else {
				invokable = Invokable.ofMethod(method);
				for (int i = 0; i < paramTypes.length; i++) {
					Class<?> paramType = paramTypes[i];
					if (!isStringWrappedType(customTypes, paramType)) continue;

					paramTypesNames[i] = String.class.getName();
					JmxCustomTypeAdapter<?> customAdapter = customTypes.get(paramType);
					if (customAdapter == null) {
						invokable = invokable.mapArgument(i, name -> parseEnum((Class<? extends Enum>) paramType, ((String) name)));
						continue;
					}

					if (customAdapter.from != null) {
						invokable = invokable.mapArgument(i, x -> customAdapter.from.apply((String) x));
					}
				}

				Class<?> returnType = method.getReturnType();
				if (customTypes.containsKey(returnType)) {
					JmxCustomTypeAdapter<?> customAdapter = customTypes.get(returnType);
					invokable = invokable.mapResult(result -> ((Function) customAdapter.to).apply(result));
				} else if (isEnumType(returnType)) {
					invokable = invokable.mapResult(anEnum -> ((Enum<?>) anEnum).name());
				}
			}

			Invokable prev = opkeyToInvokable.put(new OperationKey(opName, paramTypesNames), invokable);
			checkState(prev == null, "Ambiguous JMX operations: `" + opName + "` in " + beanClass.getName());
		}
		return opkeyToInvokable;
	}
	// endregion

	// region helper classes
	public record AttributeDescriptor(String name, Type type, Method getter, Method setter) {}

	public record OperationKey(String name, String[] argTypes) {
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof OperationKey that)) return false;

			if (!name.equals(that.name)) return false;

			return Arrays.equals(argTypes, that.argTypes);

		}

		@Override
		public int hashCode() {
			int result = name.hashCode();
			result = 31 * result + Arrays.hashCode(argTypes);
			return result;
		}
	}

	public static final class DynamicMBeanAggregator implements DynamicMBean {
		private final MBeanInfo mBeanInfo;
		private final JmxBeanAdapter adapter;
		private final List<?> beans;
		private final AttributeNodeForPojo rootNode;
		private final Map<OperationKey, Invokable> opKeyToInvokable;

		public DynamicMBeanAggregator(
			MBeanInfo mBeanInfo, JmxBeanAdapter adapter, List<?> beans, AttributeNodeForPojo rootNode,
			Map<OperationKey, Invokable> opKeyToInvokable
		) {
			this.mBeanInfo = mBeanInfo;
			this.adapter = adapter;
			this.beans = beans;

			this.rootNode = rootNode;
			this.opKeyToInvokable = opKeyToInvokable;
		}

		@Override
		public Object getAttribute(String attribute) throws MBeanException {
			Object value;
			try {
				value = rootNode.aggregateAttributes(Set.of(attribute), beans).get(attribute);
			} catch (Exception e) {
				logger.warn("Failed to fetch attribute '{}' from beans {}", attribute, beans, e);
				propagate(e);
				throw new AssertionError("Never reached");
			}

			if (value instanceof Throwable throwable) {
				logger.warn("Failed to fetch attribute '{}' from beans {}", attribute, beans, throwable);
				propagate(throwable);
			}
			return value;
		}

		@Override
		public void setAttribute(Attribute attribute) throws MBeanException {
			String attrName = attribute.getName();
			Object attrValue = attribute.getValue();

			CountDownLatch latch = new CountDownLatch(beans.size());
			Ref<Exception> exceptionRef = new Ref<>();

			for (Object bean : beans) {
				adapter.execute(bean, () -> {
					try {
						rootNode.setAttribute(attrName, attrValue, List.of(bean));
						latch.countDown();
					} catch (Exception e) {
						logger.warn("Failed to set attribute '{}' of {} with value '{}'", attrName, bean, attrValue, e);
						exceptionRef.set(e);
						latch.countDown();
					}
				});
			}

			try {
				latch.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new MBeanException(e);
			}

			Exception e = exceptionRef.get();
			if (e != null) {
				Exception actualException = e;
				if (e instanceof SetterException setterException) {
					actualException = setterException.getCausedException();
				}
				propagate(actualException);
			}
		}

		@Override
		public AttributeList getAttributes(String[] attributes) {
			AttributeList attrList = new AttributeList();
			Set<String> attrNames = Set.of(attributes);
			try {
				Map<String, Object> aggregatedAttrs = rootNode.aggregateAttributes(attrNames, beans);
				for (Map.Entry<String, Object> entry : aggregatedAttrs.entrySet()) {
					if (!(entry.getValue() instanceof Throwable)) {
						attrList.add(new Attribute(entry.getKey(), entry.getValue()));
					}
				}
			} catch (Exception e) {
				logger.warn("Failed to get attributes {} from beans {}", attrNames, beans, e);
			}
			return attrList;
		}

		@Override
		public AttributeList setAttributes(AttributeList attributes) {
			AttributeList resultList = new AttributeList();
			for (Object attr : attributes) {
				Attribute attribute = (Attribute) attr;
				try {
					setAttribute(attribute);
					resultList.add(new Attribute(attribute.getName(), attribute.getValue()));
				} catch (MBeanException ignored) {
				}
			}
			return resultList;
		}

		@Override
		public @Nullable Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException {
			Object[] args = nonNullElse(params, new Object[0]);
			String[] argTypes = nonNullElse(signature, new String[0]);
			OperationKey opkey = new OperationKey(actionName, argTypes);
			Invokable invokable = opKeyToInvokable.get(opkey);
			if (invokable == null) {
				String operationName = prettyOperationName(actionName, argTypes);
				String errorMsg = format("There is no operation \"%s\"", operationName);
				throw new RuntimeOperationsException(new IllegalArgumentException("Operation not found"), errorMsg);
			}

			return invokable.invoke(beans, adapter, args);
		}

		@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
		private static String prettyOperationName(String name, String[] argTypes) {
			StringBuilder operationName = new StringBuilder(name + "(");
			if (argTypes.length > 0) {
				for (int i = 0; i < argTypes.length - 1; i++) {
					operationName.append(argTypes[i] + ", ");
				}
				operationName.append(argTypes[argTypes.length - 1]);
			}
			operationName.append(")");
			return operationName.toString();
		}

		@Override
		public MBeanInfo getMBeanInfo() {
			return mBeanInfo;
		}
	}

	private static void propagate(Throwable e) throws MBeanException {
		if (e instanceof InvocationTargetException) {
			Throwable targetException = ((InvocationTargetException) e).getTargetException();

			if (targetException instanceof Exception) {
				throw new MBeanException((Exception) targetException);
			} else {
				throw new MBeanException(
					new Exception(format(
						"Throwable of type \"%s\" and message \"%s\" was thrown during method invocation",
						targetException.getClass().getName(), targetException.getMessage())
					)
				);
			}

		}

		if (e instanceof Exception) {
			throw new MBeanException((Exception) e);
		} else {
			throw new MBeanException(
				new Exception(format(
					"Throwable of type \"%s\" and message \"%s\" was thrown",
					e.getClass().getName(), e.getMessage())
				)
			);
		}
	}

	public static class JmxCustomTypeAdapter<T> {
		public final @Nullable Function<String, T> from;
		public final Function<T, String> to;

		public JmxCustomTypeAdapter(Function<T, String> to, @Nullable Function<String, T> from) {
			this.to = to;
			this.from = from;
		}

		public JmxCustomTypeAdapter(Function<T, String> to) {
			this.to = to;
			this.from = null;
		}
	}

	private static <E extends Enum<E>> E parseEnum(Class<E> enumClass, String name) {
		try {
			return Enum.valueOf(enumClass, name);
		} catch (IllegalArgumentException ignored) {
			try {
				return Enum.valueOf(enumClass, name.toUpperCase());
			} catch (IllegalArgumentException ignored2) {
				throw new IllegalArgumentException(format("Invalid value of enum %s: %s. Possible enum values: %s",
					enumClass.getSimpleName(), name, Arrays.toString(enumClass.getEnumConstants())));
			}
		}
	}

	private interface Invokable {
		Object invoke(List<?> beans, JmxBeanAdapter adapter, Object[] args) throws MBeanException;

		default Invokable mapArgument(int index, UnaryOperator<Object> mapper) {
			return (beans, adapter, args) -> {
				args[index] = mapper.apply(args[index]);
				return invoke(beans, adapter, args);
			};
		}

		default Invokable mapResult(UnaryOperator<Object> mapper) {
			return (beans, adapter, args) -> {
				Object result = invoke(beans, adapter, args);
				return result == null ?
					null :
					mapper.apply(result);
			};
		}

		static Invokable ofAttributeNode(AttributeNode attributeNode) {
			String name = attributeNode.getName();
			Set<String> nameSet = Set.of(name);
			return (beans, adapter, args) -> {
				if (args.length != 0) {
					throw new MBeanException(new IllegalArgumentException("Passing arguments to getter operation"));
				}

				try {
					return attributeNode.aggregateAttributes(nameSet, beans).get(name);
				} catch (Throwable e) {
					logger.warn("Failed to fetch attribute '{}' from beans {}", name, beans, e);
					propagate(e);
					return null;
				}
			};
		}

		static Invokable ofMethod(Method method) {
			//noinspection unchecked
			JmxReducer<Object> reducer = (JmxReducer<Object>) fetchReducerFrom(method);

			return (beans, adapter, args) -> {
				CountDownLatch latch = new CountDownLatch(beans.size());
				List<Object> values = new ArrayList<>(beans.size());
				Ref<Exception> exceptionRef = new Ref<>();
				for (Object bean : beans) {
					adapter.execute(bean, () -> {
						try {
							Object result = method.invoke(bean, args);
							values.add(result);
							latch.countDown();
						} catch (Exception e) {
							logger.warn("Failed to invoke method '{}' on {} with args {}", method, bean, args, e);
							exceptionRef.set(e);
							latch.countDown();
						}
					});
				}

				try {
					latch.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new MBeanException(e);
				}

				Exception e = exceptionRef.get();
				if (e != null) {
					propagate(e);
				}

				return reducer.reduce(values);
			};
		}
	}
	// endregion
}
