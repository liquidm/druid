/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.CompressionUtilsTest;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Progressable;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 */
public class JobHelperTest
{

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private HadoopDruidIndexerConfig config;
  private File tmpDir;
  private File dataFile;
  private Interval interval = Intervals.of("2014-10-22T00:00:00Z/P1D");

  @Before
  public void setup() throws Exception
  {
    tmpDir = temporaryFolder.newFile();
    dataFile = temporaryFolder.newFile();
    config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                "website",
                HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
                    new StringInputRowParser(
                        new CSVParseSpec(
                            new TimestampSpec("timestamp", "yyyyMMddHH", null),
                            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
                            null,
                            ImmutableList.of("timestamp", "host", "visited_num"),
                            false,
                            0
                        ),
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{new LongSumAggregatorFactory("visited_num", "visited_num")},
                new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, ImmutableList.of(this.interval)),
                null,
                HadoopDruidIndexerConfig.JSON_MAPPER
            ),
            new HadoopIOConfig(
                ImmutableMap.of(
                    "paths",
                    dataFile.getCanonicalPath(),
                    "type",
                    "static"
                ),
                null,
                tmpDir.getCanonicalPath()
            ),
            new HadoopTuningConfig(
                tmpDir.getCanonicalPath(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                false,
                false,
                //Map of job properties
                ImmutableMap.of(
                    "fs.s3.impl",
                    "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
                    "fs.s3.awsAccessKeyId",
                    "THISISMYACCESSKEY"
                ),
                false,
                false,
                null,
                null,
                null,
                false,
                false,
                null,
                null,
                null,
                null
            )
        )
    );
  }

  @Test
  public void testEnsurePathsAddsProperties()
  {
    HadoopDruidIndexerConfigSpy hadoopDruidIndexerConfigSpy = new HadoopDruidIndexerConfigSpy(config);
    JobHelper.ensurePaths(hadoopDruidIndexerConfigSpy);
    Map<String, String> jobProperties = hadoopDruidIndexerConfigSpy.getJobProperties();
    Assert.assertEquals(
        "fs.s3.impl property set correctly",
        "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
        jobProperties.get("fs.s3.impl")
    );
    Assert.assertEquals(
        "fs.s3.accessKeyId property set correctly",
        "THISISMYACCESSKEY",
        jobProperties.get("fs.s3.awsAccessKeyId")
    );
  }

  @Test
  public void testGoogleGetURIFromSegment() throws URISyntaxException
  {
    DataSegment segment = new DataSegment(
        "test1",
        Intervals.of("2000/3000"),
        "ver",
        ImmutableMap.of(
            "type", "google",
            "bucket", "test-test",
            "path", "tmp/foo:bar/index1.zip"
        ),
        ImmutableList.of(),
        ImmutableList.of(),
        NoneShardSpec.instance(),
        9,
        1024
    );

    Assert.assertEquals(
        new URI("gs://test-test/tmp/foo%3Abar/index1.zip"),
        JobHelper.getURIFromSegment(segment)
    );
  }

  @Test
  public void testEvilZip() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testEvilZip");

    final File evilResult = new File("/tmp/evil.txt");
    Files.deleteIfExists(evilResult.toPath());

    File evilZip = new File(tmpDir, "evil.zip");
    Files.deleteIfExists(evilZip.toPath());
    CompressionUtilsTest.makeEvilZip(evilZip);

    try {
      JobHelper.unzipNoGuava(
          new Path(evilZip.getCanonicalPath()),
          new Configuration(),
          tmpDir,
          new Progressable()
          {
            @Override
            public void progress()
            {

            }
          },
          RetryPolicies.TRY_ONCE_THEN_FAIL
      );
    }
    catch (ISE ise) {
      Assert.assertTrue(ise.getMessage().contains("does not start with outDir"));
      Assert.assertFalse("Zip exploit triggered, /tmp/evil.txt was written.", evilResult.exists());
      return;
    }
    Assert.fail("Exception was not thrown for malicious zip file");
  }

  @Test
  public void testMakeFileNamePathHadoop() throws IOException
  {
    DataSegment segment = new DataSegment(
            "test1",
            Intervals.of("2000/3000"),
            "ver",
            ImmutableMap.of(
                    "type", "google",
                    "bucket", "test-test",
                    "path", "tmp/foo:bar/index1.zip"
            ),
            ImmutableList.of(),
            ImmutableList.of(),
            NoneShardSpec.instance(),
            9,
            1024
    );

    Path result = JobHelper.makeFileNamePath(
            new Path("foo", "bar"),
            new Path("baz").getFileSystem(new Configuration()),
            segment,
            "boz",
            new HdfsDataSegmentPusherStub()
    );

    String expected = String.format(
            Locale.ENGLISH,
            "file:%s/%s",
            System.getProperty("user.dir"),
            "foo/bar/test1/20000101T000000.000Z_30000101T000000.000Z/ver/0_boz"
    );
    Assert.assertEquals(expected, result.toString());
  }

  @Test
  public void testMakeFileNamePathGeneric() throws IOException
  {
    DataSegment segment = new DataSegment(
            "test1",
            Intervals.of("2000/3000"),
            "ver",
            ImmutableMap.of(
                    "type", "google",
                    "bucket", "test-test",
                    "path", "tmp/foo:bar/index1.zip"
            ),
            ImmutableList.of(),
            ImmutableList.of(),
            NoneShardSpec.instance(),
            9,
            1024
    );

    Path result = JobHelper.makeFileNamePath(
            new Path("foo", "bar"),
            new Path("baz").getFileSystem(new Configuration()),
            segment,
            "boz",
            new GenericSegmentPusherStub()
    );

    String expected = String.format(
            Locale.ENGLISH,
            "file:%s/%s",
            System.getProperty("user.dir"),
            "foo/bar/test1/20000101T000000.000Z_30000101T000000.000Z/ver/boz"
    );
    Assert.assertEquals(expected, result.toString());
  }

  private static class HadoopDruidIndexerConfigSpy extends HadoopDruidIndexerConfig
  {

    private Map<String, String> jobProperties = new HashMap<String, String>();

    public HadoopDruidIndexerConfigSpy(HadoopDruidIndexerConfig delegate)
    {
      super(delegate.getSchema());
    }

    @Override
    public Job addInputPaths(Job job)
    {
      Configuration configuration = job.getConfiguration();
      for (Map.Entry<String, String> en : configuration) {
        jobProperties.put(en.getKey(), en.getValue());
      }
      return job;
    }

    public Map<String, String> getJobProperties()
    {
      return jobProperties;
    }
  }

  private class HdfsDataSegmentPusherStub implements DataSegmentPusher
  {
    @Override
    public String getPathForHadoop(String dataSource)
    {
      return getPathForHadoop();
    }

    @Override
    public String getPathForHadoop()
    {
      return "todo";
    }

    @Override
    public DataSegment push(final File inDir, final DataSegment segment, final boolean useUniquePath)
    {
      return new DataSegment(
              "test1",
              Intervals.of("2000/3000"),
              "ver",
              ImmutableMap.of(
                      "type", "google",
                      "bucket", "test-test",
                      "path", "tmp/foo:bar/index1.zip"
              ),
              ImmutableList.of(),
              ImmutableList.of(),
              NoneShardSpec.instance(),
              9,
              1024
      );
    }

    @Override
    public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
    {
      return ImmutableMap.of("type", "hdfs", "path", finalIndexZipFilePath.toString());
    }

    @Override
    public String getStorageDir(DataSegment segment, boolean useUniquePath)
    {
      Preconditions.checkArgument(
              !useUniquePath,
              "useUniquePath must be false for HdfsDataSegmentPusher.getStorageDir()"
      );

      return JOINER.join(
              segment.getDataSource(),
              StringUtils.format(
                      "%s_%s",
                      segment.getInterval().getStart().toString(ISODateTimeFormat.basicDateTime()),
                      segment.getInterval().getEnd().toString(ISODateTimeFormat.basicDateTime())
              ),
              segment.getVersion().replace(':', '_')
      );
    }

    @Override
    public String makeIndexPathName(DataSegment dataSegment, String indexName)
    {
      // This is only called from Hadoop batch which doesn't require unique segment paths so set useUniquePath=false
      return StringUtils.format(
              "./%s/%d_%s",
              this.getStorageDir(dataSegment, false),
              dataSegment.getShardSpec().getPartitionNum(),
              indexName
      );
    }
  }


  private class GenericSegmentPusherStub implements DataSegmentPusher
  {
    @Override
    public String getPathForHadoop(String dataSource)
    {
      return getPathForHadoop();
    }

    @Override
    public String getPathForHadoop()
    {
      return "todo";
    }

    @Override
    public DataSegment push(final File inDir, final DataSegment segment, final boolean useUniquePath)
    {
      return new DataSegment(
              "test1",
              Intervals.of("2000/3000"),
              "ver",
              ImmutableMap.of(
                      "type", "google",
                      "bucket", "test-test",
                      "path", "tmp/foo:bar/index1.zip"
              ),
              ImmutableList.of(),
              ImmutableList.of(),
              NoneShardSpec.instance(),
              9,
              1024
      );
    }

    @Override
    public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
    {
      return ImmutableMap.of("type", "hdfs", "path", finalIndexZipFilePath.toString());
    }
  }
}
