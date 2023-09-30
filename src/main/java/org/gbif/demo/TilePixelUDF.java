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
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF3;

import com.google.common.collect.Lists;

/** Returns the addresses ... todo document when ready */
public class TilePixelUDF implements UDF3<Integer, Double, Double, Row[]>, Serializable {
  static final int TILE_SIZE = 512;
  static final int BUFFER_SIZE = TILE_SIZE / 16;
  static final TileProjection projection =
      Tiles.fromEPSG("EPSG:3857", TILE_SIZE); // TODO: projections
  static final TileSchema tileSchema = TileSchema.fromSRS("EPSG:3857"); // TODO: projections

  @Override
  public Row[] call(Integer zoom, Double lat, Double lng) {
    if (projection.isPlottable(lat, lng)) {
      Double2D globalXY = projection.toGlobalPixelXY(lat, lng, zoom);

      List<String> addresses = Lists.newArrayList();

      Long2D tileXY = Tiles.toTileXY(globalXY, tileSchema, zoom, TILE_SIZE);
      Long2D localXY =
          Tiles.toTileLocalXY(
              globalXY, tileSchema, zoom, tileXY.getX(), tileXY.getY(), TILE_SIZE, BUFFER_SIZE);
      append(addresses, tileXY, localXY);

      // Readdress any coordinates that fall in the boundary region of adjacent tiles
      readdressAndAppend(addresses, globalXY, tileXY, zoom, localXY.getX(), localXY.getY());

      List<Row> rows =
          addresses.stream()
              .map(
                  s -> {
                    String p[] = s.split(",");
                    return RowFactory.create(
                        Long.valueOf(p[0]),
                        Long.valueOf(p[1]),
                        Integer.valueOf(p[2]),
                        Integer.valueOf(p[3]));
                  })
              .collect(Collectors.toList());

      return rows.toArray(new Row[rows.size()]);
    }
    return null;
  }

  /**
   * Takes the original x,y and if it lies within the boundary of adjacent tiles, adds the address
   * of the pixel.
   */
  static void readdressAndAppend(
      List<String> target, Double2D globalXY, Long2D tileXY, int zoom, long x, long y) {

    // What follows needs tests and explained. A quick hack that doesn't deal with date lines

    if (zoom > 0) {
      if (y <= BUFFER_SIZE) {
        // N
        Long2D newTileXY = new Long2D(tileXY.getX(), tileXY.getY() - 1);
        appendOnTile(target, globalXY, zoom, newTileXY);

        // NW
        if (x <= BUFFER_SIZE) {
          newTileXY = new Long2D(tileXY.getX() - 1, tileXY.getY() - 1);
          appendOnTile(target, globalXY, zoom, newTileXY);
        }

        // NE
        if (x >= TILE_SIZE - BUFFER_SIZE) {
          newTileXY = new Long2D(tileXY.getX() + 1, tileXY.getY() - 1);
          appendOnTile(target, globalXY, zoom, newTileXY);
        }
      }
      if (x >= TILE_SIZE - BUFFER_SIZE) {
        // E
        Long2D newTileXY = new Long2D(tileXY.getX() + 1, tileXY.getY());
        appendOnTile(target, globalXY, zoom, newTileXY);
      }
      if (y >= TILE_SIZE - BUFFER_SIZE) {
        // S
        Long2D newTileXY = new Long2D(tileXY.getX(), tileXY.getY() + 1);
        appendOnTile(target, globalXY, zoom, newTileXY);

        if (x <= BUFFER_SIZE) {
          // SW
          newTileXY = new Long2D(tileXY.getX() - 1, tileXY.getY() + 1);
          appendOnTile(target, globalXY, zoom, newTileXY);
        }
        if (x >= TILE_SIZE - BUFFER_SIZE) {
          // SE
          newTileXY = new Long2D(tileXY.getX() + 1, tileXY.getY() + 1);
          appendOnTile(target, globalXY, zoom, newTileXY);
        }
      }
      if (x <= BUFFER_SIZE) {
        // W
        Long2D newTileXY = new Long2D(tileXY.getX() - 1, tileXY.getY());
        appendOnTile(target, globalXY, zoom, newTileXY);
      }
    }

    // special case for when z was already 0, not covered by the rules above
    if (zoom == 0 && tileSchema.isWrapX()) {

      // E
      Long2D newTileXY = new Long2D(tileXY.getX() + 1, tileXY.getY());
      appendOnTile(target, globalXY, zoom, newTileXY);

      // W
      newTileXY = new Long2D(tileXY.getX() - 1, tileXY.getY());
      appendOnTile(target, globalXY, zoom, newTileXY);
    }
  }

  private static void appendOnTile(
      List<String> target, Double2D globalXY, int zoom, Long2D tileXY) {
    Long2D localXY =
        Tiles.toTileLocalXY(
            globalXY, tileSchema, zoom, tileXY.getX(), tileXY.getY(), TILE_SIZE, BUFFER_SIZE);
    append(target, tileXY, localXY);
  }

  /** Appends the encoded address for the given coordinates. */
  static void append(List<String> addresses, Long2D tileXY, Long2D localXY) {
    addresses.add(
        String.join(
            ",",
            String.valueOf(tileXY.getX()),
            String.valueOf(tileXY.getY()),
            String.valueOf(localXY.getX()),
            String.valueOf(localXY.getY())));
  }
}
