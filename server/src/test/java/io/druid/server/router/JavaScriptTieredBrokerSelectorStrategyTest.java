/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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

package io.druid.server.router;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;

public class JavaScriptTieredBrokerSelectorStrategyTest
{
  final TieredBrokerSelectorStrategy jsStrategy = new JavaScriptTieredBrokerSelectorStrategy(
      "function (config, query) { if (config.getTierToBrokerMap().values().size() > 0 && query.getAggregatorSpecs && query.getAggregatorSpecs().size() <= 2) { return config.getTierToBrokerMap().values().toArray()[0] } else { return config.getDefaultBrokerServiceName() } }"
  );

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        jsStrategy,
        mapper.readValue(
            mapper.writeValueAsString(jsStrategy),
            JavaScriptTieredBrokerSelectorStrategy.class
        )
    );
  }

  @Test
  public void testGetBrokerServiceName() throws Exception
  {
    final LinkedHashMap<String, String> tierBrokerMap = new LinkedHashMap<>();
    tierBrokerMap.put("fast", "druid/fastBroker");
    tierBrokerMap.put("slow", "druid/broker");

    final TieredBrokerConfig tieredBrokerConfig = new TieredBrokerConfig()
    {
      @Override
      public String getDefaultBrokerServiceName()
      {
        return "druid/broker";
      }

      @Override
      public LinkedHashMap<String, String> getTierToBrokerMap()
      {
        return tierBrokerMap;
      }
    };

    final Druids.TimeseriesQueryBuilder queryBuilder = Druids.newTimeseriesQueryBuilder().dataSource("test")
                                        .intervals("2014/2015")
                                        .aggregators(
                                            ImmutableList.<AggregatorFactory>of(
                                                new CountAggregatorFactory("count")
                                            )
                                        );

    Assert.assertEquals(
        Optional.of("druid/fastBroker"),
        jsStrategy.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder.build()
        )
    );


    Assert.assertEquals(
        Optional.of("druid/broker"),
        jsStrategy.getBrokerServiceName(
            tieredBrokerConfig,
            Druids.newTimeBoundaryQueryBuilder().dataSource("test").bound("maxTime").build()
        )
    );

    Assert.assertEquals(
        Optional.of("druid/broker"),
        jsStrategy.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder.aggregators(
                ImmutableList.of(
                    new CountAggregatorFactory("count"),
                    new LongSumAggregatorFactory("longSum", "a"),
                    new DoubleSumAggregatorFactory("doubleSum", "b")
                )
            ).build()
        )
    );

  }
}
