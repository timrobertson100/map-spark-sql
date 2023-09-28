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

import org.gbif.maps.common.projection.*;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.sql.api.java.UDF13;

import com.google.common.collect.Lists;

import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

/** Returns the map keys for the record. */
public class MapKeysUDF
    implements UDF13<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            String,
            String,
            String,
            String,
            WrappedArray<String>,
            String[]>,
        Serializable {
  @Override
  public String[] call(
      Integer kingdomKey,
      Integer phylumKey,
      Integer classKey,
      Integer orderKey,
      Integer familyKey,
      Integer genusKey,
      Integer speciesKey,
      Integer taxonKey,
      String datasetKey,
      String publishingOrgKey,
      String countryCode,
      String publishingCountry,
      WrappedArray<String> networkKeys) {
    List<String> keys = Lists.newArrayList("ALL:0");
    appendNonNull(keys, "TAXON", kingdomKey);
    appendNonNull(keys, "TAXON", phylumKey);
    appendNonNull(keys, "TAXON", classKey);
    appendNonNull(keys, "TAXON", orderKey);
    appendNonNull(keys, "TAXON", familyKey);
    appendNonNull(keys, "TAXON", genusKey);
    appendNonNull(keys, "TAXON", speciesKey);
    appendNonNull(keys, "TAXON", taxonKey);
    appendNonNull(keys, "DATASET", datasetKey);
    appendNonNull(keys, "PUBLISHER", publishingOrgKey);
    appendNonNull(keys, "COUNTRY", countryCode);
    appendNonNull(keys, "PUBLISHING_COUNTRY", publishingCountry);
    if (networkKeys != null && networkKeys.size() > 0) {
      for (String n : JavaConversions.seqAsJavaList(networkKeys)) {
        appendNonNull(keys, "NETWORK", n);
      }
    }
    return keys.toArray(new String[keys.size()]);
  }

  static void appendNonNull(List<String> target, String prefix, Object l) {
    if (l != null) target.add(prefix + ":" + l);
  }
}
