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

import java.io.Serializable;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;

/** Generates a vector tile from the structured input */
public class VectorTileUDF implements UDF1<Row, byte[]>, Serializable {

  @Override
  public byte[] call(Row tileData) {
    return null;
  }
}
