/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.benchmark;

import java.io.IOException;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RuntimeComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.join.BuildFirstHashJoinOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Ignore;
import org.junit.Test;

public class StreamTaskBenchmark extends StreamTaskTestBase<Tuple2<Integer, String>> {

	public static final TupleTypeInfo<Tuple2<Integer, String>> tupleTypeInfo =
			new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

	private static final int MEMORY_MANAGER_SIZE = 1024 * 1024 * 3;

	private static final int NETWORK_BUFFER_SIZE = 1024 * 32;

	private static long memorySize = 104857600;

	private static int size = 16;

	private static int range = 100_000;

	private static int count = 5_000_000;

	private static void getParameters() {
		memorySize = Long.valueOf(System.getProperty("memory", "104857600")); // 100M
		size = Integer.valueOf(System.getProperty("size", "16"));
		range = Integer.valueOf(System.getProperty("range", "100000"));
		count = Integer.valueOf(System.getProperty("count", "1000000"));
		System.out.println("run with parameters:\n"
				+ "record size: " + size  + "\n"
				+ "memory size: " + memorySize / 1024 / 1024 + " mb\n"
				+ "key range: " + range + "\n");
	}

	@Ignore
	@Test
	public void testEmptyOperator() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

		final OneInputStreamTask<Tuple2<Integer, String>, Tuple2<Integer, String>> task = new OneInputStreamTask<>();
		setTask(task);

		MutableObjectIterator<StreamElement> input =
				new RandomStreamElementGenerator(
						new RandomTupleGenerator(size, count, range, false),
						tupleTypeSerializer);
		StreamElement reuse = (new StreamElementSerializer<>(tupleTypeSerializer)).createInstance();
		TestingInputGate<StreamElement> inputGate = new TestingInputGate<>(
				input, new StreamElementSerializer<>(tupleTypeSerializer));
		setInputGates(new InputGate[]{inputGate}, tupleTypeSerializer);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		setOutputWriter(writer, tupleTypeSerializer);

		streamConfig.setStreamOperator(new StreamMap<>(new DummyMapFunction()));

		long start = System.nanoTime();
		task.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\n" +
						"write count: %d\n" +
						"average: %.2f\n",
				inputGate.getProcessedCount(), writer.getCount().get(),
				inputGate.getProcessedCount() / sec));
	}


	@Ignore
	@Test
	public void testMapFixed() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

		final OneInputStreamTask<Tuple2<Integer, String>, Tuple2<Integer, String>> task = new OneInputStreamTask<>();
		setTask(task);

		int stringSize = size - 4;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < stringSize; ++i) {
			sb.append('a');
		}
		Tuple2<Integer, String> fixedRecord = new Tuple2<>(1, sb.toString());
		StreamElement fixedElement = new StreamRecord<>(fixedRecord);
		TypeSerializer<StreamElement> streamElementSerializer = new StreamElementSerializer<>(tupleTypeSerializer);

		FixedInputGate<StreamElement> inputGate = new FixedInputGate<>(count, fixedElement, streamElementSerializer);
		setInputGates(new InputGate[]{inputGate}, tupleTypeSerializer);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		setOutputWriter(writer, tupleTypeSerializer);

		streamConfig.setStreamOperator(new StreamMap<>(new DummyMapFunction()));

		long start = System.nanoTime();
		task.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\n" +
						"write count: %d\n" +
						"average: %.2f\n",
				inputGate.getProcessedCount(), writer.getCount().get(),
				inputGate.getProcessedCount() / sec));
	}

	@Ignore
	@Test
	public void testJoinOperator() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

		final TwoInputStreamTask<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> task = new TwoInputStreamTask<>();
		setTask(task);

		MutableObjectIterator<StreamElement> input1 =
				new RandomStreamElementGenerator(
						new RandomTupleGenerator(size, range, range, true),
						tupleTypeSerializer);
		MutableObjectIterator<StreamElement> input2 =
				new RandomStreamElementGenerator(
						new RandomTupleGenerator(size, count, range, false),
						tupleTypeSerializer);

		TestingInputGate<StreamElement> inputGate1 = new TestingInputGate<>(
				input1, new StreamElementSerializer<>(tupleTypeSerializer));
		TestingInputGate<StreamElement> inputGate2 = new TestingInputGate<>(
				input2, new StreamElementSerializer<>(tupleTypeSerializer));

		setInputGates(new InputGate[]{inputGate1, inputGate2}, tupleTypeSerializer);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		setOutputWriter(writer, tupleTypeSerializer);

		streamConfig.setStreamOperator(new BuildFirstHashJoinOperator<>(new DummyJoinFunction(), false, false));
		streamConfig.setCheckpointMode(CheckpointingMode.BATCH);

		RuntimeComparatorFactory compFact1 = new RuntimeComparatorFactory(
				new TupleComparator<>(
						new int[]{0},
						new TypeComparator<?>[]{new IntComparator(true)},
						new TypeSerializer<?>[]{IntSerializer.INSTANCE}
				));
		RuntimeComparatorFactory compFact2 = new RuntimeComparatorFactory(
				new TupleComparator<>(
						new int[]{0},
						new TypeComparator<?>[]{new IntComparator(true)},
						new TypeSerializer<?>[]{IntSerializer.INSTANCE}
				));
		streamConfig.setComparatorIn1(compFact1);
		streamConfig.setComparatorIn2(compFact2);
		streamConfig.setPairComparator(new RuntimePairComparatorFactory());

		long start = System.nanoTime();
		task.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		int joinCount = ((DummyJoinFunction) (((BuildFirstHashJoinOperator) task.getHeadOperator()).getUserFunction())).joinCount;

		System.out.println(
				String.format("build count: %d\nprobe count: %d\njoined count: %d\naverage: %.2f\n",
						inputGate1.getProcessedCount(), inputGate2.getProcessedCount(),
						joinCount, joinCount / sec));
	}

	private static class DummyJoinFunction extends RichFlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> {

		private int joinCount = 0;

		@Override
		public void join(Tuple2<Integer, String> first, Tuple2<Integer, String> second, Collector<Tuple2<Integer, String>> out) throws Exception {
			joinCount++;
//			if (joinCount % 100 == 0) {
//				System.out.println("joined, " +
//						"left: (" + first.getField(0, IntValue.class) + ", " + first.getField(1, StringValue.class) +
//						"), right: (" + second.getField(0, IntValue.class) + ", " + second.getField(1, StringValue.class) + ")");
//			}
			out.collect(second);
		}


		@Override
		public void close() throws Exception {
			System.out.println("joined count: " + joinCount);
		}
	}

	private static class DummyMapFunction extends RichMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

		private int mapCount = 0;

		@Override
		public Tuple2<Integer, String> map(Tuple2<Integer, String> value) throws Exception {
			mapCount++;
//			if (mapCount % 1000 == 0) {
//				System.out.println("map: (" + value.getField(0, IntValue.class)
//						+ ", " + value.getField(1, StringValue.class) + ")");
//			}
			return value;
		}

		@Override
		public void close() throws Exception {
			System.out.println("mapped count: " + mapCount);
		}
	}

	private static class RandomStreamElementGenerator implements MutableObjectIterator<StreamElement> {

		private final MutableObjectIterator<Tuple2<Integer, String>> iterator;

		private final StreamElementSerializer<Tuple2<Integer, String>> serializer;

		private final Tuple2<Integer, String> reuseRecord = new Tuple2<>();

		public RandomStreamElementGenerator(
				MutableObjectIterator<Tuple2<Integer, String>> iterator,
				TypeSerializer<Tuple2<Integer, String>> typeSerializer) {
			this.iterator = iterator;
			this.serializer = new StreamElementSerializer<>(typeSerializer);
		}

		@Override
		public StreamElement next(StreamElement reuse) throws IOException {
			Tuple2<Integer, String> record = iterator.next(reuseRecord);
			if (record != null) {
				reuse.asRecord().replace(record);
				return reuse;
			} else {
				return null;
			}
		}

		@Override
		public StreamElement next() throws IOException {
			StreamElement record = serializer.createInstance();
			return next(record);
		}
	}

	public static void main(String[] args) throws Exception {
		StreamTaskBenchmark benchmark = new StreamTaskBenchmark();

		String test = System.getProperty("test");
		if (test == null) {
			System.err.println("You should specify a test run via -Dtest=...");
			System.exit(1);
		}

		getParameters();

		boolean warmup = Boolean.valueOf(System.getProperty("warmup", "false"));
		if (warmup) {
			System.out.println("============= Warm up JIT ==================");
			for (int i = 0; i < 10; ++i) {
				run(benchmark, test);
				benchmark.shutdownIOManager();
				benchmark.shutdownMemoryManager();
			}
		}

//		System.err.println("Prepare to start test!!!");
//		Thread.sleep(30000);

		System.out.println("============= Test ==================");
		run(benchmark, test);
		benchmark.shutdownIOManager();
		benchmark.shutdownMemoryManager();
	}

	private static void run(StreamTaskBenchmark benchmark, String testName) throws Exception {
		if (testName.equalsIgnoreCase("testEmptyOperator")) {
			benchmark.testEmptyOperator();
		} else if (testName.equalsIgnoreCase("testMapFixed")) {
			benchmark.testMapFixed();
		} else if (testName.equalsIgnoreCase("testJoinOperator")) {
			benchmark.testJoinOperator();
		} else {
			System.err.println("Unknown test name:" + testName);
			System.exit(1);
		}
	}


}
