/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.demo.experiments;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * A combine avro keyvalue file input format that can combine small avro files into mappers.
 *
 * @param <K> The type of the Avro key to read.
 */
public class CombineAvroKeyFileInputFormat<K>
    extends CombineFileInputFormat<AvroKey<K>, NullWritable> {

  @Override
  public RecordReader<AvroKey<K>, NullWritable> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    return new CombineFileRecordReader(
        (CombineFileSplit) inputSplit,
        taskAttemptContext,
        CombineAvroKeyFileInputFormat.AvroKeyFileRecordReaderWrapper.class);
  }

  /**
   * A record reader that may be passed to <code>CombineFileRecordReader</code> so that it can be
   * used in a <code>CombineFileInputFormat</code>-equivalent for <code>AvroKeyInputFormat</code>.
   */
  private static class AvroKeyFileRecordReaderWrapper<K>
      extends CombineFileRecordReaderWrapper<AvroKey<K>, NullWritable> {
    // this constructor signature is required by CombineFileRecordReader
    public AvroKeyFileRecordReaderWrapper(
        CombineFileSplit split, TaskAttemptContext context, Integer idx)
        throws IOException, InterruptedException {
      super(new AvroKeyInputFormat<>(), split, context, idx);
    }
  }
}
