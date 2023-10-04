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
package org.gbif.demo;

import org.gbif.maps.common.hbase.ModulusSalt;

import org.apache.spark.Partitioner;

/**
 * A partitioner that puts data destined for the same HBase region together based on the prefix of
 * the key salt.
 */
public class SaltPrefixPartitioner extends Partitioner {
  final int numPartitions;

  public SaltPrefixPartitioner(int saltLength) {
    numPartitions = new Double(Math.pow(10, saltLength)).intValue();
  }

  @Override
  public int getPartition(Object key) {
    return ModulusSalt.saltFrom(key.toString());
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }
}
