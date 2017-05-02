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

package org.apache.flink.streaming.api.operators.join;

import java.util.List;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.hash.MutableHashTable2;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 *
 * @param <IN1>
 * @param <IN2>
 * @param <OUT>
 */
public class BuildFirstHashJoinOperator<IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, FlatJoinFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private final boolean probeSideOuterJoin;

	private final boolean buildSideOuterJoin;

	private transient TypeSerializer<IN1> buildSideSerializer;

	private transient TypeSerializer<IN2> probeSideSerializer;

	private transient TypeComparator<IN1> buildSideComparator;

	private transient TypeComparator<IN2> probeSideComparator;

	private transient MutableHashTable2<IN1, IN2> hashJoin;

	private transient Collector<OUT> collector;

	public BuildFirstHashJoinOperator(
			FlatJoinFunction<IN1, IN2, OUT> function,
			boolean probeSideOuterJoin,
			boolean buildSideOuterJoin) {
		super(function);

		this.probeSideOuterJoin = probeSideOuterJoin;
		this.buildSideOuterJoin = buildSideOuterJoin;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config,
			Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		this.collector = new OutputWrapper(this.output);
	}

	@Override
	public void open() throws Exception {
		super.open();

		// TODO: proper initialization
		this.buildSideSerializer = config.getTypeSerializerIn1(getUserCodeClassloader());
		this.probeSideSerializer = config.getTypeSerializerIn2(getUserCodeClassloader());
		this.buildSideComparator = (TypeComparator<IN1>) config.getComparatorIn1(getUserCodeClassloader()).createComparator();
		this.probeSideComparator = (TypeComparator<IN2>) config.getComparatorIn2(getUserCodeClassloader()).createComparator();
		TypePairComparatorFactory<IN1, IN2> pairComparatorFactory = config.getPairComparatorFactory(getUserCodeClassloader());
		TypePairComparator<IN2, IN1> pairComparator = pairComparatorFactory.createComparator21(buildSideComparator, probeSideComparator);
		MemoryManager memManager = container.getMemoryManager();
		IOManager ioManager = container.getIOManager();
		double memoryFraction = 1.0;
		boolean useBloomFilters = true;

		final int numPages = memManager.computeNumberOfPages(memoryFraction);
		final List<MemorySegment> memorySegments = memManager.allocatePages(container, numPages);

		hashJoin = new MutableHashTable2<>(buildSideSerializer, probeSideSerializer,
				buildSideComparator, probeSideComparator, pairComparator,
				memorySegments, ioManager, useBloomFilters);
		hashJoin.open(buildSideOuterJoin);
	}

	@Override
	public InputSelection firstInputSelection() {
		return InputSelection.FIRST;
	}

	@Override
	public InputSelection processElement1(StreamRecord<IN1> element) throws Exception {
		hashJoin.addBuildRecord(element.getValue());
//		Record record = (Record) element.getValue();
//		System.out.println(
//				"build: (" + record.getField(0, IntValue.class) + ", " + record.getField(1, StringValue.class) + ")");
		return InputSelection.FIRST;
	}

	@Override
	public InputSelection processElement2(StreamRecord<IN2> element) throws Exception {
//		Record record = (Record) element.getValue();
//		System.out.println(
//				"probe: (" + record.getField(0, IntValue.class) + ", " + record.getField(1, StringValue.class) + ")");
		if (hashJoin.probe(element.getValue())) {
			doJoin();
		}
		return InputSelection.SECOND;
	}

	@Override
	public InputSelection endInput1() throws Exception {
		hashJoin.buildFinish();
		return InputSelection.SECOND;
	}

	@Override
	public void close() throws Exception {
		super.close();
		hashJoin.close();
	}

	@Override
	public InputSelection endInput2() throws Exception {
		hashJoin.probeFinish();
		while (hashJoin.moreRecord()) {
			doJoin();
		}
		return InputSelection.NONE;
	}

	private void doJoin() throws Exception {
		final MutableObjectIterator<IN1> buildSideIterator = this.hashJoin.getBuildSideIterator();
		final IN2 probeRecord = this.hashJoin.getCurrentProbeRecord();
		IN1 nextBuildSideRecord = buildSideIterator.next();

		// get the first build side value
		if (probeRecord != null && nextBuildSideRecord != null) {
			IN1 tmpRec;

			// check if there is another build-side value
			if ((tmpRec = buildSideIterator.next()) != null) {
				// more than one build-side value --> copy the probe side
				IN2 probeCopy;
				probeCopy = this.probeSideSerializer.copy(probeRecord);

				// call match on the first pair
				userFunction.join(nextBuildSideRecord, probeCopy, collector);

				// call match on the second pair
				probeCopy = this.probeSideSerializer.copy(probeRecord);
				userFunction.join(tmpRec, probeCopy, collector);

				while ((nextBuildSideRecord = buildSideIterator.next()) != null) {
					// call match on the next pair
					// make sure we restore the value of the probe side record
					probeCopy = this.probeSideSerializer.copy(probeRecord);
					userFunction.join(nextBuildSideRecord, probeCopy, collector);
				}
			} else {
				// only single pair matches
				userFunction.join(nextBuildSideRecord, probeRecord, collector);
			}
		} else {
			// while probe side outer join, join current probe record with null.
			if (probeSideOuterJoin && probeRecord != null && nextBuildSideRecord == null) {
				userFunction.join(null, probeRecord, collector);
			}

			// while build side outer join, iterate all build records which have not been probed before,
			// and join with null.
			if (buildSideOuterJoin && probeRecord == null && nextBuildSideRecord != null) {
				userFunction.join(nextBuildSideRecord, null, collector);

				while ((nextBuildSideRecord = buildSideIterator.next()) != null) {
					userFunction.join(nextBuildSideRecord, null, collector);
				}
			}
		}
	}

	private class OutputWrapper implements Collector<OUT> {

		private final StreamRecord<OUT> element = new StreamRecord<>(null);

		private final Output<StreamRecord<OUT>> underlyingOutput;

		OutputWrapper(Output<StreamRecord<OUT>> output) {
			this.underlyingOutput = output;
		}

		@Override
		public void collect(OUT record) {
			underlyingOutput.collect(element.replace(record));
		}

		@Override
		public void close() {
			underlyingOutput.close();
		}
	}
}
