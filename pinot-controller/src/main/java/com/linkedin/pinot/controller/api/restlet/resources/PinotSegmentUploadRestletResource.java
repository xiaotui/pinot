/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.restlet.resources;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Response;
import com.linkedin.pinot.common.restlet.swagger.Responses;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcher;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.FileUploadUtils.FileUploadType;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.controller.validation.StorageQuotaChecker;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.joda.time.Interval;
import org.json.JSONArray;
import org.json.JSONException;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Post;
import org.restlet.util.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * sample curl call : curl -F campaignInsights_adsAnalysis-bmCamp_11=@campaignInsights_adsAnalysis-bmCamp_11      http://localhost:8998/segments
 *
 */
public class PinotSegmentUploadRestletResource extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentUploadRestletResource.class);
  private static final String RESTLET_HTTP_HEADERS = "org.restlet.http.headers";

  private final File baseDataDir;
  private final File tempDir;
  private final File tempUntarredPath;
  private final String vip;

  public PinotSegmentUploadRestletResource() throws IOException {
    baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "fileUploadTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
    tempUntarredPath = new File(tempDir, "untarred");
    if (!tempUntarredPath.exists()) {
      tempUntarredPath.mkdirs();
    }

    vip = StringUtil.join("://", _controllerConf.getControllerVipProtocol(),
        StringUtil.join(":", _controllerConf.getControllerVipHost(), _controllerConf.getControllerPort()));
    LOGGER.info("controller download url base is : " + vip);
  }

  @Override
  public Representation get() {
    Representation presentation = null;
    try {
      final String tableName = (String) getRequest().getAttributes().get("tableName");
      final String segmentName = (String) getRequest().getAttributes().get("segmentName");
      final String active = getReference().getQueryAsForm().getValues("active");

      if ((tableName == null) && (segmentName == null)) {
        return getAllSegments();

      } else if ((tableName != null) && (segmentName == null)) {
        return getSegmentsForTable(tableName, !"false".equalsIgnoreCase(active));
      }

      presentation = getSegmentFile(tableName, segmentName);
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing get request", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_GET_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @HttpVerb("get")
  @Summary("Downloads a segment")
  @Tags({"segment", "table"})
  @Paths({
      "/segments/{tableName}/{segmentName}"
  })
  @Responses({
      @Response(statusCode = "200", description = "A segment file in Pinot format"),
      @Response(statusCode = "404", description = "The segment file or table does not exist")
  })
  private Representation getSegmentFile(
      @Parameter(name = "tableName", in = "path", description = "The name of the table in which the segment resides", required = true)
      String tableName,
      @Parameter(name = "segmentName", in = "path", description = "The name of the segment to download", required = true)
      String segmentName) {
    Representation presentation;
    try {
      segmentName = URLDecoder.decode(segmentName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("UTF-8 encoding should always be supported", e);
    }
    final File dataFile = new File(baseDataDir, StringUtil.join("/", tableName, segmentName));
    if (dataFile.exists()) {
      presentation = new FileRepresentation(dataFile, MediaType.ALL, 0);
    } else {
      setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      presentation = new StringRepresentation("Table or segment is not found!");
    }
    return presentation;
  }

  @HttpVerb("get")
  @Summary("Lists all segments for a given table")
  @Tags({"segment", "table"})
  @Paths({
      "/segments/{tableName}",
      "/segments/{tableName}/"
  })
  @Responses({
      @Response(statusCode = "200", description = "A list of all segments for the specified table"),
      @Response(statusCode = "404", description = "The segment file or table does not exist")
  })
  private Representation getSegmentsForTable(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to list segments", required = true)
      String tableName,
      @Parameter(name = "active", in = "query", description = "true = show active segments (in Helix), false = all segments (on filesystem)", required = false)
      boolean activeOnly
      ) {
    Representation presentation;
    final JSONArray ret = new JSONArray();

    if (activeOnly) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      List<String> segmentList = _pinotHelixResourceManager.getAllSegmentsForResource(offlineTableName);
      ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();

      for (String segmentName : segmentList) {
        OfflineSegmentZKMetadata offlineSegmentZKMetadata =
            ZKMetadataProvider.getOfflineSegmentZKMetadata(propertyStore, tableName, segmentName);
        ret.put(offlineSegmentZKMetadata.getDownloadUrl());
      }
    } else {
      File tableDir = new File(baseDataDir, tableName);

      if (tableDir.exists()) {
        for (final File file : tableDir.listFiles()) {
          final String url = "http://" + StringUtil
              .join(":", _controllerConf.getControllerVipHost(), _controllerConf.getControllerPort()) + "/segments/" + tableName + "/" + file.getName();
          ret.put(url);
        }
      } else {
        LOGGER.error("Error: Table {} not found.", tableName);
        setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      }
    }

    presentation = new StringRepresentation(ret.toString());
    return presentation;
  }

  @HttpVerb("get")
  @Summary("Lists all segments")
  @Tags({"segment"})
  @Paths({
      "/segments",
      "/segments/"
  })
  @Responses({
      @Response(statusCode = "200", description = "A list of all segments for the all tables")
  })
  private Representation getAllSegments() {
    Representation presentation;
    final JSONArray ret = new JSONArray();
    for (final File file : baseDataDir.listFiles()) {
      final String fileName = file.getName();
      if (fileName.equalsIgnoreCase("fileUploadTemp") || fileName.equalsIgnoreCase("schemasTemp")) {
        continue;
      }

      final String url =
          "http://" + StringUtil.join(":", _controllerConf.getControllerVipHost(), _controllerConf.getControllerPort()) + "/segments/"
              + fileName;
      ret.put(url);
    }
    presentation = new StringRepresentation(ret.toString());
    return presentation;
  }

  @Override
  @Post
  public Representation post(Representation entity) {
    Representation rep = null;
    File tmpSegmentDir = null;
    File dataFile = null;
    try {
      // 0/ Get upload type, if it's uri, then download it, otherwise, get the tar from the request.
      Series headers = (Series) getRequestAttributes().get(RESTLET_HTTP_HEADERS);
      String uploadTypeStr = headers.getFirstValue(FileUploadUtils.UPLOAD_TYPE);
      FileUploadType uploadType = null;
      try {
        uploadType = (uploadTypeStr == null) ? FileUploadType.getDefaultUploadType() : FileUploadType.valueOf(uploadTypeStr);
      } catch (Exception e) {
        uploadType = FileUploadType.getDefaultUploadType();
      }
      String downloadURI = null;
      boolean found = false;
      switch (uploadType) {
        case URI:
        case JSON:
          // Download segment from the given Uri
          try {
            downloadURI = getDownloadUri(uploadType, headers, entity);
          } catch (Exception e) {
            String errorMsg =
                String.format("Failed to get download Uri for upload file type: %s, with error %s",
                    uploadType, e.getMessage());
            LOGGER.warn(errorMsg);
            JSONObject errorMsgInJson = getErrorMsgInJson(errorMsg);
            ControllerRestApplication.getControllerMetrics()
                .addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
            setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
            return new StringRepresentation(errorMsgInJson.toJSONString(),
                MediaType.APPLICATION_JSON);
          }

          SegmentFetcher segmentFetcher = null;
          // Get segmentFetcher based on uri parsed from download uri
          try {
            segmentFetcher = SegmentFetcherFactory.getSegmentFetcherBasedOnURI(downloadURI);
          } catch (Exception e) {
            String errorMsg =
                String.format("Failed to get SegmentFetcher from download Uri: %s", downloadURI);
            LOGGER.warn(errorMsg);
            JSONObject errorMsgInJson = getErrorMsgInJson(errorMsg);
            ControllerRestApplication.getControllerMetrics()
                .addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
            setStatus(Status.SERVER_ERROR_INTERNAL);
            return new StringRepresentation(errorMsgInJson.toJSONString(),
                MediaType.APPLICATION_JSON);
          }
          // Download segment tar to local.
          dataFile = new File(tempDir, "tmp-" + System.nanoTime());
          try {
            segmentFetcher.fetchSegmentToLocal(downloadURI, dataFile);
          } catch (Exception e) {
            String errorMsg =
                String.format("Failed to fetch segment tar from download Uri: %s to %s",
                    downloadURI, dataFile.toString());
            LOGGER.warn(errorMsg);
            JSONObject errorMsgInJson = getErrorMsgInJson(errorMsg);
            ControllerRestApplication.getControllerMetrics()
                .addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
            setStatus(Status.SERVER_ERROR_INTERNAL);
            return new StringRepresentation(errorMsgInJson.toJSONString(),
                MediaType.APPLICATION_JSON);
          }
          if (dataFile.exists() && dataFile.length() > 0) {
            found = true;
          }
          break;
        case TAR:
        default:
          // 1/ Create a factory for disk-based file items
          final DiskFileItemFactory factory = new DiskFileItemFactory();

          // 2/ Create a new file upload handler based on the Restlet
          // FileUpload extension that will parse Restlet requests and
          // generates FileItems.
          final RestletFileUpload upload = new RestletFileUpload(factory);
          final List<FileItem> items;

          // 3/ Request is parsed by the handler which generates a
          // list of FileItems
          items = upload.parseRequest(getRequest());

          for (final Iterator<FileItem> it = items.iterator(); it.hasNext() && !found;) {
            final FileItem fi = it.next();
            if (fi.getFieldName() != null) {
              found = true;
              dataFile = new File(tempDir, fi.getFieldName());
              fi.write(dataFile);
            }
          }
      }
      // Once handled, the content of the uploaded file is sent
      // back to the client.
      if (found) {
        // Create a new representation based on disk file.
        // The content is arbitrarily sent as plain text.
        rep = new StringRepresentation(dataFile + " sucessfully uploaded", MediaType.TEXT_PLAIN);
        tmpSegmentDir =
            new File(tempUntarredPath, dataFile.getName() + "-" + _controllerConf.getControllerHost() + "_"
                + _controllerConf.getControllerPort() + "-" + System.currentTimeMillis());
        LOGGER.info("Untar segment to temp dir: " + tmpSegmentDir);
        if (tmpSegmentDir.exists()) {
          FileUtils.deleteDirectory(tmpSegmentDir);
        }
        if (!tmpSegmentDir.exists()) {
          tmpSegmentDir.mkdirs();
        }
        // While there is TarGzCompressionUtils.unTarOneFile, we use unTar here to unpack all files
        // in the segment in order to ensure the segment is not corrupted
        TarGzCompressionUtils.unTar(dataFile, tmpSegmentDir);
        File segmentFile = tmpSegmentDir.listFiles()[0];
        String clientIpAddress = getClientInfo().getAddress();
        String clientAddress = InetAddress.getByName(clientIpAddress).getHostName();
        LOGGER.info("Processing upload request for segment '{}' from client '{}'", segmentFile.getName(), clientAddress);
        return uploadSegment(segmentFile, dataFile, downloadURI);
      } else {
        // Some problem occurs, sent back a simple line of text.
        String errorMsg = "No file was uploaded";
        LOGGER.warn(errorMsg);
        JSONObject errorMsgInJson = getErrorMsgInJson(errorMsg );
        rep = new StringRepresentation(errorMsgInJson.toJSONString(), MediaType.APPLICATION_JSON);
        ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
        setStatus(Status.SERVER_ERROR_INTERNAL);
      }
    } catch (final Exception e) {
      rep = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception in file upload", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    } finally {
      if ((tmpSegmentDir != null) && tmpSegmentDir.exists()) {
        try {
          FileUtils.deleteDirectory(tmpSegmentDir);
        } catch (final IOException e) {
          LOGGER.error("Caught exception in file upload", e);
          ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
          setStatus(Status.SERVER_ERROR_INTERNAL);
        }
      }
      if ((dataFile != null) && dataFile.exists()) {
        FileUtils.deleteQuietly(dataFile);
      }
    }
    return rep;
  }

  private String getDownloadUri(FileUploadType uploadType, Series headers, Representation entity) throws Exception {
    switch (uploadType) {
    case URI:
      return headers.getFirstValue(FileUploadUtils.DOWNLOAD_URI);
    case JSON:
      // Get segmentJsonStr
      String segmentJsonStr = entity.getText();
      JSONObject segmentJson = JSONObject.parseObject(segmentJsonStr);
      // Download segment from the given Uri
      return segmentJson.getString(CommonConstants.Segment.Offline.DOWNLOAD_URL);
    default:
      break;
    }
    throw new UnsupportedOperationException("Not support getDownloadUri method for upload type - " + uploadType);
  }

  @HttpVerb("post")
  @Summary("Uploads a segment")
  @Tags({"segment"})
  @Paths({
      "/segments",
      "/segments/"
  })
  @Responses({
      @Response(statusCode = "200", description = "The segment was successfully uploaded"),
      @Response(statusCode = "403", description = "Forbidden operation typically because it exceeds configured quota"),
      @Response(statusCode = "500", description = "There was an error when uploading the segment")
  })
  private Representation uploadSegment(File indexDir, File dataFile, String downloadUrl)
      throws ConfigurationException, IOException, JSONException {
    final SegmentMetadata metadata = new SegmentMetadataImpl(indexDir);
    final File tableDir = new File(baseDataDir, metadata.getTableName());
    File segmentFile = new File(tableDir, dataFile.getName());
    StorageQuotaChecker.QuotaCheckerResponse quotaResponse = checkStorageQuota(indexDir, metadata);
    if (!quotaResponse.isSegmentWithinQuota) {
      // this is not an "error" hence we don't increment segment upload errors
      setStatus(Status.CLIENT_ERROR_FORBIDDEN);
      StringRepresentation repr = new StringRepresentation("{\"error\" : \"" + quotaResponse.reason + "\"}");
      repr.setMediaType(MediaType.APPLICATION_JSON);
      return repr;
    }

    if (segmentFile.exists()) {
      FileUtils.deleteQuietly(segmentFile);
    }
    FileUtils.moveFile(dataFile, segmentFile);

    PinotResourceManagerResponse response;
    if (!isSegmentTimeValid(metadata)) {
      response = new PinotResourceManagerResponse("Invalid segment start/end time", false);
    } else {
      if (downloadUrl == null) {
        downloadUrl = constructDownloadUrl(metadata.getTableName(), dataFile.getName());
      }
      response = _pinotHelixResourceManager.addSegment(metadata, downloadUrl);
    }

    if (response.isSuccessfull()) {
      setStatus(Status.SUCCESS_OK);
    } else {
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }

    return new StringRepresentation(response.toJSON().toString());
  }

  /**
   * Returns true if:
   * - Segment does not have a start/end time, OR
   * - The start/end time are in a valid range (Jan 01 1971 - Jan 01, 2071)
   * @param metadata
   * @return
   */
  private boolean isSegmentTimeValid(SegmentMetadata metadata) {
    Interval interval = metadata.getTimeInterval();
    if (interval == null) {
      return true;
    }

    long startMillis = interval.getStartMillis();
    long endMillis = interval.getEndMillis();

    if (!TimeUtils.timeValueInValidRange(startMillis) || !TimeUtils.timeValueInValidRange(endMillis)) {
      Date startDate = new Date(interval.getStartMillis());
      Date endDate = new Date(interval.getEndMillis());

      Date minDate = new Date(TimeUtils.getValidMinTimeMillis());
      Date maxDate = new Date(TimeUtils.getValidMaxTimeMillis());

      LOGGER.error("Invalid start time '{}' or end time '{}' for segment, must be between '{}' and '{}'", startDate,
          endDate, minDate, maxDate);
      return false;
    }

    return true;
  }

  /**
   * Returns true if segment start and end time are between a valid range, or if
   * segment does not have a time interval.
   * The current valid range is between 1971 and 2071.
   * @param metadata
   * @return
   */
  private boolean validateSegmentTimeRange(SegmentMetadata metadata) {
    Interval timeInterval = metadata.getTimeInterval();
    return (timeInterval == null || (TimeUtils.timeValueInValidRange(timeInterval.getStartMillis())) && TimeUtils
        .timeValueInValidRange(timeInterval.getEndMillis()));
  }

  /**
   * URI Mappings:
   * - "/segments/{tableName}/{segmentName}", "/segments/{tableName}/{segmentName}/":
   *   Delete the specified segment from the specified table.
   *
   * - "/segments/{tableName}/", "/segments/{tableName}":
   *   Delete all the segments from the specified table.
   *
   * {@inheritDoc}
   * @see org.restlet.resource.ServerResource#delete()
   */
  @Override
  @Delete
  public Representation delete() {
    Representation rep;
    try {
      final String tableName = (String) getRequest().getAttributes().get("tableName");
      final String segmentName = (String) getRequest().getAttributes().get("segmentName");

      LOGGER.info("Getting segment deletion request, tableName: " + tableName + " segmentName: " + segmentName);
      rep = deleteOneSegment(tableName, segmentName);
    } catch (final Exception e) {
      rep = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing delete request", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_DELETE_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return rep;
  }

  @HttpVerb("delete")
  @Summary("Delete a segment from a table")
  @Tags({"segment", "table"})
  @Paths({ "/segments/{tableName}/{segmentName}", "/segments/{tableName}/{segmentName}/" })
  private Representation deleteOneSegment(
      @Parameter(name = "tableName", in = "path", description = "The name of the table in which the segment resides",
          required = true) String tableName,
      @Parameter(name = "segmentName", in = "path", description = "The name of the segment to delete",
          required = true) String segmentName)
      throws JsonProcessingException, JSONException {

    return new PinotSegmentRestletResource().toggleSegmentState(tableName, segmentName, "drop", null);
  }

  @HttpVerb("delete")
  @Summary("Delete *ALL* segments from a table")
  @Tags({"segment", "table"})
  @Paths({ "/segments/{tableName}", "/segments/{tableName}/" })
  private Representation deleteAllSegments(
      @Parameter(name = "tableName", in = "path", description = "The name of the table in which the segment resides",
          required = true) String tableName)
      throws JsonProcessingException, JSONException {

    return new PinotSegmentRestletResource().toggleSegmentState(tableName, null, "drop", null);
  }

  public String constructDownloadUrl(String tableName, String segmentName) {
    try {
      return StringUtil.join("/", vip, "segments", tableName, URLEncoder.encode(segmentName, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      // Shouldn't happen
      throw new AssertionError("UTF-8 encoding should always be supported", e);
    }
  }

  /**
   * check if the segment represented by segmentFile is within the storage quota
   * @param segmentFile untarred segment. This should not be null.
   *                    segmentFile must exist on disk and must be a directory
   * @param metadata segment metadata. This should not be null
   */
  private StorageQuotaChecker.QuotaCheckerResponse checkStorageQuota(@Nonnull File segmentFile, @Nonnull SegmentMetadata metadata) {
    TableSizeReader tableSizeReader = new TableSizeReader(executor, connectionManager, _pinotHelixResourceManager);
    AbstractTableConfig offlineTableConfig = ZKMetadataProvider
        .getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), metadata.getTableName());
    StorageQuotaChecker quotaChecker = new StorageQuotaChecker(offlineTableConfig, tableSizeReader);
    String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(metadata.getTableName());
    return quotaChecker.isSegmentStorageWithinQuota(segmentFile, offlineTableName,
        metadata.getName(), _controllerConf.getServerAdminRequestTimeoutSeconds());
  }

}
