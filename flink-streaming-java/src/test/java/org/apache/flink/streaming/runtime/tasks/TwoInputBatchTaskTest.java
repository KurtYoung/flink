/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RuntimeComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.operators.hash.NonReusingHashJoinIteratorITCase;
import org.apache.flink.runtime.testutils.recordutils.RecordComparatorFactory;
import org.apache.flink.runtime.testutils.recordutils.RecordPairComparatorFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.co.CoStreamMap;
import org.apache.flink.streaming.api.operators.join.BuildFirstHashJoinOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for {@link TwoInputStreamTask}. Theses tests
 * implicitly also test the {@link org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor}.
 *
 * <p>
 * Note:<br>
 * We only use a {@link CoStreamMap} operator here. We also test the individual operators but Map is
 * used as a representative to test TwoInputStreamTask, since TwoInputStreamTask is used for all
 * TwoInputStreamOperators.
 */
public class TwoInputBatchTaskTest {

	/**
	 * This test verifies that open() and close() are correctly called. This test also verifies
	 * that timestamps of emitted elements are correct. {@link CoStreamMap} assigns the input
	 * timestamp to emitted elements.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testOpenCloseAndTimestamps() throws Exception {
		final TwoInputStreamTask<String, Integer, String> coMapTask = new TwoInputStreamTask<String, Integer, String>();
		final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness = new TwoInputStreamTaskTestHarness<String, Integer, String>(coMapTask, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<String, Integer, String>(new TestOpenCloseMapFunction());
		streamConfig.setStreamOperator(coMapOperator);
		streamConfig.setCheckpointMode(CheckpointingMode.BATCH);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processElement(new StreamRecord<String>("Hello", initialTime + 1), 0, 0);
		expectedOutput.add(new StreamRecord<String>("Hello", initialTime + 1));

		// wait until the input is processed to ensure ordering of the output
		testHarness.waitForInputProcessing();

		testHarness.processElement(new StreamRecord<Integer>(1337, initialTime + 2), 1, 0);

		expectedOutput.add(new StreamRecord<String>("1337", initialTime + 2));

		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		Assert.assertTrue("RichFunction methods where not called.", TestOpenCloseMapFunction.closeCalled);

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	/**
	 * This test verifies that watermarks and stream statuses are correctly forwarded. This also checks whether
	 * watermarks are forwarded only when we have received watermarks from all inputs. The
	 * forwarded watermark must be the minimum of the watermarks of all active inputs.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testWatermarkAndStreamStatusForwarding() throws Exception {
		final TwoInputStreamTask<String, Integer, String> coMapTask = new TwoInputStreamTask<String, Integer, String>();
		final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness =
			new TwoInputStreamTaskTestHarness<String, Integer, String>(
				coMapTask, 2, 2, new int[] {1, 2},
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<String, Integer, String>(new IdentityMap());
		streamConfig.setStreamOperator(coMapOperator);
		streamConfig.setCheckpointMode(CheckpointingMode.BATCH);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();
		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processElement(new Watermark(initialTime), 0, 0);
		testHarness.processElement(new Watermark(initialTime), 0, 1);

		testHarness.processElement(new Watermark(initialTime), 1, 0);


		// now the output should still be empty
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(new Watermark(initialTime), 1, 1);

		// now the watermark should have propagated, Map simply forward Watermarks
		testHarness.waitForInputProcessing();
		expectedOutput.add(new Watermark(initialTime));
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		// contrary to checkpoint barriers these elements are not blocked by watermarks
		testHarness.processElement(new StreamRecord<String>("Hello", initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<Integer>(42, initialTime), 1, 1);
		expectedOutput.add(new StreamRecord<String>("Hello", initialTime));
		expectedOutput.add(new StreamRecord<String>("42", initialTime));

		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(new Watermark(initialTime + 4), 0, 0);
		testHarness.processElement(new Watermark(initialTime + 3), 0, 1);
		testHarness.processElement(new Watermark(initialTime + 3), 1, 0);
		testHarness.processElement(new Watermark(initialTime + 2), 1, 1);

		// check whether we get the minimum of all the watermarks, this must also only occur in
		// the output after the two StreamRecords
		expectedOutput.add(new Watermark(initialTime + 2));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());


		// advance watermark from one of the inputs, now we should get a new one since the
		// minimum increases
		testHarness.processElement(new Watermark(initialTime + 4), 1, 1);
		testHarness.waitForInputProcessing();
		expectedOutput.add(new Watermark(initialTime + 3));
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		// advance the other two inputs, now we should get a new one since the
		// minimum increases again
		testHarness.processElement(new Watermark(initialTime + 4), 0, 1);
		testHarness.processElement(new Watermark(initialTime + 4), 1, 0);
		testHarness.waitForInputProcessing();
		expectedOutput.add(new Watermark(initialTime + 4));
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		// test whether idle input channels are acknowledged correctly when forwarding watermarks
		testHarness.processElement(StreamStatus.IDLE, 0, 1);
		testHarness.processElement(StreamStatus.IDLE, 1, 0);
		testHarness.processElement(new Watermark(initialTime + 6), 0, 0);
		testHarness.processElement(new Watermark(initialTime + 5), 1, 1); // this watermark should be advanced first
		testHarness.processElement(StreamStatus.IDLE, 1, 1); // once this is acknowledged,
		                                                     // watermark (initial + 6) should be forwarded
		testHarness.waitForInputProcessing();
		expectedOutput.add(new Watermark(initialTime + 5));
		// We don't expect to see Watermark(6) here because the idle status of one
		// input doesn't propagate to the other input. That is, if input 1 is at WM 6 and input
		// two was at WM 5 before going to IDLE then the output watermark will not jump to WM 6.
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		// make all input channels idle and check that the operator's idle status is forwarded
		testHarness.processElement(StreamStatus.IDLE, 0, 0);
		testHarness.waitForInputProcessing();
		expectedOutput.add(StreamStatus.IDLE);
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		// make some input channels active again and check that the operator's active status is forwarded only once
		testHarness.processElement(StreamStatus.ACTIVE, 1, 0);
		testHarness.processElement(StreamStatus.ACTIVE, 0, 1);
		testHarness.waitForInputProcessing();
		expectedOutput.add(StreamStatus.ACTIVE);
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		List<String> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
		Assert.assertEquals(2, resultElements.size());
	}

	@Test
	public void testHashJoin() throws Exception {
		TupleTypeInfo<Tuple2<String, String>> tupleTypeInfo =
				new TupleTypeInfo<>(
						BasicTypeInfo.STRING_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO
				);
		final TwoInputStreamTask<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> joinTask
				= new TwoInputStreamTask<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>();
		final TwoInputStreamTaskTestHarness<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> testHarness
				= new TwoInputStreamTaskTestHarness<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>(joinTask, 2, 2, new int[] {1, 2}, tupleTypeInfo, tupleTypeInfo, tupleTypeInfo);
		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		Map<String, Collection<NonReusingHashJoinIteratorITCase.TupleMatch>> tmp = null;
		TupleMatchAddJoin joinFunction = new TupleMatchAddJoin(tmp);
		BuildFirstHashJoinOperator<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> joinOperator = new BuildFirstHashJoinOperator<>(joinFunction, true, false);
		streamConfig.setStreamOperator(joinOperator);
		streamConfig.setCheckpointMode(CheckpointingMode.BATCH);
		RuntimeComparatorFactory compFact = new RuntimeComparatorFactory(new TupleComparator<>(
				new int[]{0},
				new TypeComparator<?>[]{new StringComparator(true)},
				new TypeSerializer<?>[]{StringSerializer.INSTANCE}
		));
		streamConfig.setComparatorIn1(compFact);
		streamConfig.setComparatorIn2(compFact);
		streamConfig.setPairComparator(new RuntimePairComparatorFactory());

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();
		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processElement(new StreamRecord<Tuple2<String, String>>(new Tuple2<String, String>("a", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<Tuple2<String, String>>(new Tuple2<String, String>("b", "1"), initialTime), 0, 1);
		testHarness.endInput(0);
		testHarness.waitForInputProcessing();

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());

		testHarness.processElement(new StreamRecord<Tuple2<String, String>>(new Tuple2<String, String>("a", "2"), initialTime), 1, 1);
		expectedOutput.add(new StreamRecord<Tuple2<String, String>>(new Tuple2<String, String>("a", "02")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());

		testHarness.processElement(new StreamRecord<Tuple2<String, String>>(new Tuple2<String, String>("c", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<Tuple2<String, String>>(new Tuple2<String, String>("b", "4"), initialTime), 1, 0);
		expectedOutput.add(new StreamRecord<Tuple2<String, String>>(new Tuple2<String, String>("b", "14")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());


		testHarness.endInput();
		testHarness.waitForTaskCompletion();
	}

	/**
	 * This test verifies that checkpoint barriers are correctly forwarded.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testCheckpointBarriers() throws Exception {
		final TwoInputStreamTask<String, Integer, String> coMapTask = new TwoInputStreamTask<String, Integer, String>();
		final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness = new TwoInputStreamTaskTestHarness<String, Integer, String>(coMapTask, 2, 2, new int[] {1, 2}, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<String, Integer, String>(new IdentityMap());
		streamConfig.setStreamOperator(coMapOperator);
		streamConfig.setCheckpointMode(CheckpointingMode.BATCH);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();
		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forFullCheckpoint()), 0, 0);

		// This element should be buffered since we received a checkpoint barrier on
		// this input
		testHarness.processElement(new StreamRecord<String>("Hello-0-0", initialTime), 0, 0);
		expectedOutput.add(new StreamRecord<String>("Hello-0-0", initialTime));

		// This one should go through
		testHarness.processElement(new StreamRecord<String>("Ciao-0-0", initialTime), 0, 1);
		expectedOutput.add(new StreamRecord<String>("Ciao-0-0", initialTime));

		testHarness.waitForInputProcessing();

		// These elements should be forwarded, since we did not yet receive a checkpoint barrier
		// on that input, only add to same input, otherwise we would not know the ordering
		// of the output since the Task might read the inputs in any order
		testHarness.processElement(new StreamRecord<Integer>(11, initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<Integer>(111, initialTime), 1, 1);
		expectedOutput.add(new StreamRecord<String>("11", initialTime));
		expectedOutput.add(new StreamRecord<String>("111", initialTime));

		testHarness.waitForInputProcessing();

		// Wait to allow input to end up in the output.
		// TODO Use count down latches instead as a cleaner solution
		for (int i = 0; i < 20; ++i) {
			if (testHarness.getOutput().size() >= expectedOutput.size()) {
				break;
			} else {
				Thread.sleep(100);
			}
		}

		// we should not yet see the barrier, only the two elements from non-blocked input
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
			expectedOutput,
			testHarness.getOutput());

		testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forFullCheckpoint()), 0, 1);
		testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forFullCheckpoint()), 1, 0);
		testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forFullCheckpoint()), 1, 1);

		testHarness.waitForInputProcessing();
		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		// now we should see the barrier and after that the buffered elements
		expectedOutput.add(new CheckpointBarrier(0, 0, CheckpointOptions.forFullCheckpoint()));

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());


		List<String> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
		Assert.assertEquals(4, resultElements.size());
	}

	// This must only be used in one test, otherwise the static fields will be changed
	// by several tests concurrently
	private static class TestOpenCloseMapFunction extends RichCoMapFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;

		public static boolean openCalled = false;
		public static boolean closeCalled = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			if (closeCalled) {
				Assert.fail("Close called before open.");
			}
			openCalled = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
			if (!openCalled) {
				Assert.fail("Open was not called before close.");
			}
			closeCalled = true;
		}

		@Override
		public String map1(String value) throws Exception {
			if (!openCalled) {
				Assert.fail("Open was not called before run.");
			}
			return value;
		}

		@Override
		public String map2(Integer value) throws Exception {
			if (!openCalled) {
				Assert.fail("Open was not called before run.");
			}
			return value.toString();
		}
	}

	private static class IdentityMap implements CoMapFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map1(String value) throws Exception {
			return value;
		}

		@Override
		public String map2(Integer value) throws Exception {

			return value.toString();
		}
	}

	static final class TupleMatchAddJoin implements FlatJoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>
	{
		private final Map<String, Collection<NonReusingHashJoinIteratorITCase.TupleMatch>> toRemoveFrom;

		protected TupleMatchAddJoin(Map<String, Collection<NonReusingHashJoinIteratorITCase.TupleMatch>> map) {
			this.toRemoveFrom = map;
		}

		@Override
		public void join(Tuple2<String, String> rec1, Tuple2<String, String> rec2, Collector<Tuple2<String, String>> out) throws Exception
		{
			if (rec1 == null || rec2 == null) {
				return;
			}
			String key1 = rec1.f0;
			String key2 = rec2.f0;
			String value1 = rec1.f1;
			String value2 = rec2.f1;
			if (key1.equals(key2)) {
				out.collect(new Tuple2<String, String>(key1, value1 + value2));
			}
			//System.out.println("rec1 key = "+ key1 + ", rec1 value= " + value1 + "  rec2 key= "+key2 +  ", rec2 value " + value2);
		}
	}
}

