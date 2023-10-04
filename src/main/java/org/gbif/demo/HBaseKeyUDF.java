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

import java.io.Serializable;

import org.apache.spark.sql.api.java.UDF4;

/** Generates a salted HBase key for the given map tile coordinates */
public class HBaseKeyUDF implements UDF4<String, Integer, Integer, Integer, String>, Serializable {
  final ModulusSalt salter;

  public HBaseKeyUDF(ModulusSalt s) {
    salter = s;
  }

  @Override
  public String call(String mapKey, Integer z, Integer x, Integer y) {
    return salter.saltToString(String.format("%s:%d:%d:%d", mapKey, z, x, y));
  }
}
