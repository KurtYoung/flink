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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.XORShiftRandom;

public class RandomRecordGenerator implements MutableObjectIterator<Record> {

	private IntValue key = new IntValue();
	private StringValue value = new StringValue();

	private final int recordSize;
	private final int count;
	private int generatedCount;

	private final int keyRange;
	private final boolean keyDistinct;

	private final int keySize = 4;
	private final int stringSize;
	private char[] charBuffer;

	private final Random random;

	private ArrayList<Integer> preGeneratedKeys;

	public RandomRecordGenerator(int recordSize, int count, int keyRange, boolean keyDistinct) {
		Preconditions.checkArgument(recordSize > keySize);

		this.recordSize = recordSize;
		this.count = count;
		this.generatedCount = 0;

		this.keyRange = keyRange;
		this.keyDistinct = keyDistinct;

		this.stringSize = recordSize - keySize;
		this.charBuffer = new char[stringSize];
		for (int i = 0; i < stringSize; ++i) {
			this.charBuffer[i] = 'a';
		}
		this.random = new XORShiftRandom();

		if (keyDistinct) {
			Preconditions.checkArgument(keyRange == count);
			preGeneratedKeys = new ArrayList<>(keyRange);
			for (int i = 0; i < keyRange; ++i) {
				preGeneratedKeys.add(i, i);
			}
			Collections.shuffle(preGeneratedKeys, random);
		}
	}

	@Override
	public Record next(Record reuse) throws IOException {
		if (generatedCount >= count) {
			return null;
		}

		if (keyDistinct) {
			key.setValue(preGeneratedKeys.get(generatedCount));
		} else {
			key.setValue(random.nextInt(keyRange));
		}
		int g = random.nextInt(26);
		for (int i = 0; i < stringSize; ++i) {
			charBuffer[i] = (char) ('a' + g);
		}
		value.setValue(charBuffer, 0, stringSize);

		generatedCount++;

		reuse.setField(0, key);
		reuse.setField(1, value);
		reuse.updateBinaryRepresenation();
		return reuse;
	}

	@Override
	public Record next() throws IOException {
		key = new IntValue();
		value = new StringValue();
		return next(new Record(2));
	}
}
