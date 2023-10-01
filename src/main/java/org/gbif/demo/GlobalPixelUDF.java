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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF3;

/** Returns the addresses ... todo document when ready */
public class GlobalPixelUDF implements UDF3<Integer, Double, Double, Row>, Serializable {
  static final int TILE_SIZE = 512;
  static final TileProjection projection =
      Tiles.fromEPSG("EPSG:3857", TILE_SIZE); // TODO: projections

  @Override
  public Row call(Integer zoom, Double lat, Double lng) {
    if (projection.isPlottable(lat, lng)) {

      // Global coordinates for the projection at the maximum zoom
      Double2D globalXY = projection.toGlobalPixelXY(lat, lng, zoom);
      long x = Double.valueOf(globalXY.getX()).longValue();
      long y = Double.valueOf(globalXY.getY()).longValue();
      return RowFactory.create(zoom, Long.valueOf(x), Long.valueOf(y));
    }
    return null;
  }
}
