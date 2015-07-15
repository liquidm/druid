/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
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

package io.druid.storage.azure;

import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import com.metamx.common.CompressionUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class AzureDataSegmentPuller implements DataSegmentPuller
{
  private static final Logger log = new Logger(AzureDataSegmentPuller.class);

  private final AzureStorage azureStorage;

  @Inject
  public AzureDataSegmentPuller(
      AzureStorage azureStorage
  )
  {
    this.azureStorage = azureStorage;
  }

  public com.metamx.common.FileUtils.FileCopyResult getSegmentFiles(
      final String containerName,
      final String blobPath,
      final File outDir
  )
      throws SegmentLoadingException
  {
    prepareOutDir(outDir);

    try {

      final ByteSource byteSource = new AzureByteSource(azureStorage, containerName, blobPath);
      final com.metamx.common.FileUtils.FileCopyResult result = CompressionUtils.unzip(
          byteSource,
          outDir,
          AzureUtils.AZURE_RETRY,
          true
      );

      log.info("Loaded %d bytes from [%s] to [%s]", result.size(), blobPath, outDir.getAbsolutePath());
      return result;
    }
    catch (IOException e) {
      try {
        FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        log.warn(
            ioe,
            "Failed to remove output directory [%s] for segment pulled from [%s]",
            outDir.getAbsolutePath(),
            blobPath
        );
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }

  }

  @Override
  public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException
  {

    final Map<String, Object> loadSpec = segment.getLoadSpec();
    final String containerName = MapUtils.getString(loadSpec, "containerName");
    final String blobPath = MapUtils.getString(loadSpec, "blobPath");

    getSegmentFiles(containerName, blobPath, outDir);
  }

  public void prepareOutDir(final File outDir) throws ISE
  {
    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    if (!outDir.isDirectory()) {
      throw new ISE("[%s] must be a directory.", outDir);
    }

  }
}
