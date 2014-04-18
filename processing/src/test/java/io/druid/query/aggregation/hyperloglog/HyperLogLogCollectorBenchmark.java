/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.hyperloglog;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

public class HyperLogLogCollectorBenchmark extends SimpleBenchmark
{
  private final HashFunction fn = Hashing.murmur3_128();

  private final List<HyperLogLogCollector> collectors = Lists.newLinkedList();

  @Param({"true"}) boolean targetIsDirect;
  @Param({"default", "random", "0"}) String alignment;

  boolean alignSource;
  boolean alignTarget;

  int CACHE_LINE = 64;

  ByteBuffer chunk;
  final int count = 100_000;
  int[] positions = new int[count];
  int[] sizes = new int[count];

  @Override
  protected void setUp() throws Exception
  {
    boolean random = false;
    Random rand = new Random(0);
    int defaultOffset = 0;

    switch(alignment) {
      case "default":
        alignSource = false;
        alignTarget = false;
        break;

      case "random":
        random = true;
        break;

      default:
        defaultOffset = Integer.parseInt(alignment);
    }

    int val = 0;
    chunk = ByteBuffers.allocateAlignedByteBuffer(
        (HyperLogLogCollector.getLatestNumBytesForDenseStorage() + CACHE_LINE
         + CACHE_LINE) * count, CACHE_LINE
    );

    int pos = 0;
    for(int i = 0; i < count; ++i) {
      HyperLogLogCollector c = HyperLogLogCollector.makeLatestCollector();
      for(int k = 0; k < 40; ++k) c.add(fn.hashInt(++val).asBytes());
      final ByteBuffer sparseHeapCopy = c.toByteBuffer();
      int size = sparseHeapCopy.remaining();

      final ByteBuffer buf;

      final int offset = random ? (int)(rand.nextDouble() * 64) : defaultOffset;

      if(alignSource && (pos % CACHE_LINE) != offset) {
        pos += (pos % CACHE_LINE) < offset ? offset - (pos % CACHE_LINE) : (CACHE_LINE + offset - pos % CACHE_LINE);
      }

      positions[i] = pos;
      sizes[i] = size;

      chunk.limit(pos + size);
      chunk.position(pos);
      buf = chunk.duplicate();
      buf.mark();

      pos += size;

      buf.put(sparseHeapCopy);
      buf.reset();
      collectors.add(HyperLogLogCollector.makeCollector(buf));
    }
  }

  private ByteBuffer allocateEmptyHLLBuffer(boolean direct, boolean aligned, int offset)
  {
    final int size = HyperLogLogCollector.getLatestNumBytesForDenseStorage();
    final byte[] EMPTY_BYTES = HyperLogLogCollector.makeEmptyVersionedByteArray();
    final ByteBuffer buf;
    if(direct) {
      if(aligned) {
        buf = ByteBuffers.allocateAlignedByteBuffer(size + offset, CACHE_LINE);
        buf.position(offset);
        buf.mark();
        buf.limit(size + offset);
      } else {
        buf = ByteBuffer.allocateDirect(size);
        buf.mark();
        buf.limit(size);
      }

      buf.put(EMPTY_BYTES);
      buf.reset();
    }
    else {
      buf = ByteBuffer.allocate(size);
      buf.limit(size);
      buf.put(EMPTY_BYTES);
      buf.rewind();
    }
    return buf;
  }

  public double timeFold(int reps) throws Exception
  {
    final ByteBuffer buf = allocateEmptyHLLBuffer(targetIsDirect, alignTarget, 0);

    for (int k = 0; k < reps; ++k) {
      for(int i = 0; i < count; ++i) {
        final int pos = positions[i];
        final int size = sizes[i];

        HyperLogLogCollector.makeCollector(
            (ByteBuffer) buf.duplicate().position(0).limit(
                HyperLogLogCollector.getLatestNumBytesForDenseStorage()
            )
        ).fold(
            HyperLogLogCollector.makeCollector(
                (ByteBuffer) chunk.duplicate().limit(pos + size).position(pos)
            )
        );
      }
    }
    return HyperLogLogCollector.makeCollector(buf.duplicate()).estimateCardinality();
  }

  public static void main(String[] args) throws Exception {
    Runner.main(HyperLogLogCollectorBenchmark.class, args);
  }
}

class ByteBuffers {
  private static final Unsafe UNSAFE;
  private static final long ADDRESS_OFFSET;

  static {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);
      ADDRESS_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
    } catch (Exception e) {
      throw new RuntimeException("Cannot access Unsafe methods", e);
    }
  }

  public static long getAddress(ByteBuffer buf) {
    return UNSAFE.getLong(buf, ADDRESS_OFFSET);
  }

  public static ByteBuffer allocateAlignedByteBuffer(int capacity, int align) {
    Preconditions.checkArgument(Long.bitCount(align) == 1, "Alignment must be a power of 2");
    final ByteBuffer buf = ByteBuffer.allocateDirect(capacity + align);
    long address = getAddress(buf);
    if ((address & (align - 1)) == 0) {
      buf.limit(capacity);
    } else {
      int offset = (int) (align - (address & (align - 1)));
      buf.position(offset);
      buf.limit(offset + capacity);
    }
    return buf.slice();
  }
}
