/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.loader;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.ForwardIndexType;
import com.linkedin.pinot.core.segment.creator.InvertedIndexType;
import com.linkedin.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectory.Writer;

public class SchemaChangeHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaChangeHandler.class);

  private File indexDir;
  private SegmentMetadataImpl metadata;
  private Optional<IndexLoadingConfigMetadata> indexConfig;
  private Schema schema;
  private SegmentDirectory segmentDirectory;

  enum Action {
    ADD_DIMENSION,
    ADD_METRIC, // present in schema but not in segment
    UPDATE_DIMENSION_DEFAULT_VALUE,
    UPDATE_METRIC_DEFAULT_VALUE // present in schema & segment but default values don't match
  }

  SchemaChangeHandler(File indexDir, SegmentMetadataImpl metadata,
      IndexLoadingConfigMetadata indexConfig, Schema schema) {
    Preconditions.checkNotNull(indexDir);
    Preconditions.checkNotNull(metadata);
    Preconditions.checkState(indexDir.exists(), "Segment directory: {} does not exist", indexDir);
    Preconditions.checkState(indexDir.isDirectory(), "Segment path: {} is not a directory",
        indexDir);

    this.indexDir = indexDir;
    this.metadata = metadata;
    this.indexConfig = Optional.fromNullable(indexConfig);
    this.schema = schema;
    // always use mmap. That's safest and performs well without impact from
    // -Xmx params
    // This is not final load of the segment
    segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, metadata, ReadMode.mmap);
  }

  @SuppressWarnings("unchecked")
  public SegmentMetadataImpl process() throws Exception {
    if (schema == null) {
      return metadata;
    }
    // compute the action needed for each column
    Map<String, Action> schemaUpdateActionMap = computeSchemaUpdateAction();
    //return if schema and segment are in sync
    if (schemaUpdateActionMap.isEmpty()) {
      return metadata;
    }
    
    // perform the necessary step for each column

    File tmpIndexDir = new File(indexDir + "/tmp");
    FileUtils.deleteDirectory(tmpIndexDir);
    tmpIndexDir.mkdirs();
    PropertiesConfiguration newMetadataProperties =
        metadata.getSegmentMetadataPropertiesConfiguration();
    // create the dictionary and forward index in tmpIndexDir,
    for (String column : schemaUpdateActionMap.keySet()) {
      // this method updates the metadataProperties
      createColumnWithDefaultValue(column, newMetadataProperties, tmpIndexDir);
    }
    // update the actual segment
    Writer segmentWriter = segmentDirectory.createWriter();
    List<String> newDimensionColumns = new ArrayList<>();
    List<String> newMetricColumns = new ArrayList<>();
    for (String column : schemaUpdateActionMap.keySet()) {
      Action action = schemaUpdateActionMap.get(column);
      if (action == Action.ADD_DIMENSION) {
        newDimensionColumns.add(column);
      } else if (action == Action.ADD_METRIC) {
        newMetricColumns.add(column);
      }
      if (action == Action.UPDATE_DIMENSION_DEFAULT_VALUE
          || action == Action.UPDATE_METRIC_DEFAULT_VALUE) {
        if (segmentWriter.isIndexRemovalSupported()) {
          segmentWriter.removeIndex(column, ColumnIndexType.DICTIONARY);
          segmentWriter.removeIndex(column, ColumnIndexType.FORWARD_INDEX);
        } else {
          LOGGER.warn("Updating default value is not supported in {} segment version",
              metadata.getVersion());
        }
      }
      // add dictionary
      String dictionaryFileName = metadata.getDictionaryFileName(column, metadata.getVersion());
      File dictionaryFile = new File(tmpIndexDir, dictionaryFileName);
      PinotDataBuffer dictionaryBuffer = segmentWriter.newIndexFor(column,
          ColumnIndexType.DICTIONARY, (int) dictionaryFile.length());
      dictionaryBuffer.readFrom(dictionaryFile);
      dictionaryBuffer.close();
      // add forward index
      String fwdIndexFileName = metadata.getForwardIndexFileName(column, metadata.getVersion());
      File fwdIndexFile = new File(tmpIndexDir, fwdIndexFileName);
      PinotDataBuffer fwdIndexBuffer = segmentWriter.newIndexFor(column,
          ColumnIndexType.FORWARD_INDEX, (int) fwdIndexFile.length());
      fwdIndexBuffer.readFrom(fwdIndexFile);
      fwdIndexBuffer.close();
    }
    segmentWriter.close();
    if (newDimensionColumns.size() > 0) {
      List<String> list =
          newMetadataProperties.getList(V1Constants.MetadataKeys.Segment.DIMENSIONS);
      list.addAll(newDimensionColumns);
      newMetadataProperties.setProperty(V1Constants.MetadataKeys.Segment.DIMENSIONS, list);
    }
    if (newMetricColumns.size() > 0) {
      List<String> list = newMetadataProperties.getList(V1Constants.MetadataKeys.Segment.METRICS);
      list.addAll(newMetricColumns);
      newMetadataProperties.setProperty(V1Constants.MetadataKeys.Segment.METRICS, list);
    }
    // create a back up.
    File backupMetadataFile =
        new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME + ".bak");
    File origMetadataFile = new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    // If the back up file already exists, don't create it again
    if (!backupMetadataFile.exists()) {
      FileUtils.copyFile(origMetadataFile, backupMetadataFile);
    }
    // generate the new metadata file
    File newMetadataFile = new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    newMetadataProperties.save(newMetadataFile);

    // delete the temporary directory
    FileUtils.deleteDirectory(tmpIndexDir);
    return new SegmentMetadataImpl(indexDir);
  }

  /**
   * Computes the action needed for each column.
   * This method compares the column metadata across schema and segment
   * @return Action Map for each column
   */
  private Map<String, Action> computeSchemaUpdateAction() {
    Collection<String> columnsInSchema = schema.getColumnNames();
    Map<String, Action> schemaUpdateActionMap = new LinkedHashMap<>();
    for (String column : columnsInSchema) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      ColumnMetadata columnMetadata = metadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        boolean isAutoGenerated = columnMetadata.isAutoGenerated();
        String defaultValueFromMetadata = String.valueOf(columnMetadata.getDefaultNullValue());
        String defaultValueFromSchema = String.valueOf(fieldSpec.getDefaultNullValue());
        if (isAutoGenerated && !defaultValueFromMetadata.equals(defaultValueFromSchema)) {
          if (fieldSpec.getFieldType() == FieldType.DIMENSION) {
            schemaUpdateActionMap.put(column, Action.UPDATE_DIMENSION_DEFAULT_VALUE);
          } else if (fieldSpec.getFieldType() == FieldType.METRIC) {
            schemaUpdateActionMap.put(column, Action.UPDATE_METRIC_DEFAULT_VALUE);
          } else {
            throw new UnsupportedOperationException(
                "Adding time column is not supported in schema evolution");
          }
        }
      } else {
        if (fieldSpec.getFieldType() == FieldType.DIMENSION) {
          schemaUpdateActionMap.put(column, Action.ADD_DIMENSION);
        } else if (fieldSpec.getFieldType() == FieldType.METRIC) {
          schemaUpdateActionMap.put(column, Action.ADD_METRIC);
        } else {
          throw new UnsupportedOperationException(
              "Adding time column is not supported in schema evolution");
        }
      }
    }
    return schemaUpdateActionMap;
  }

  @SuppressWarnings("unchecked")
  private void createColumnWithDefaultValue(String column,
      PropertiesConfiguration metadataProperties, File tmpIndexDir) throws Exception {
    int totalDocs = metadata.getTotalDocs();
    int totalRawDocs = metadata.getTotalRawDocs();
    int totalAggDocs = metadata.getTotalDocs() - metadata.getTotalRawDocs();
    // we need only 1 bit since we have only 1 value in the forward index
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    int dictionaryElementSize;
    Object min = fieldSpec.getDefaultNullValue();
    Object max = fieldSpec.getDefaultNullValue();
    Object sortedArray;
    switch (fieldSpec.getDataType()) {
    case STRING:
    case BOOLEAN:
      byte[] bytes = fieldSpec.getDefaultNullValue().toString().getBytes();
      dictionaryElementSize = bytes.length;
      sortedArray = new String[] {
          fieldSpec.getDefaultNullValue().toString()
      };
      break;
    case SHORT:
      sortedArray = new short[] {
          Short.valueOf(fieldSpec.getDefaultNullValue().toString())
      };
      dictionaryElementSize = fieldSpec.getDataType().size();
      break;
    case INT:
      sortedArray = new int[] {
          Integer.valueOf(fieldSpec.getDefaultNullValue().toString())
      };
      dictionaryElementSize = fieldSpec.getDataType().size();
      break;
    case FLOAT:
      sortedArray = new float[] {
          Float.valueOf(fieldSpec.getDefaultNullValue().toString())
      };
      dictionaryElementSize = fieldSpec.getDataType().size();
      break;
    case DOUBLE:
      sortedArray = new Double[] {
          Double.valueOf(fieldSpec.getDefaultNullValue().toString())
      };
      dictionaryElementSize = fieldSpec.getDataType().size();
      break;
    case LONG:
      sortedArray = new Long[] {
          Long.valueOf(fieldSpec.getDefaultNullValue().toString())
      };
      dictionaryElementSize = fieldSpec.getDataType().size();
      break;
    default:
      throw new UnsupportedOperationException(
          "Schema evolution not supported for data type:" + fieldSpec.getDataType());
    }
    boolean createDictionary = true;
    boolean isSortedColumn = createDictionary;
    boolean hasNulls = false;
    int totalNumberOfEntries = 0;
    int maxNumberOfMultiValueElements = 0;
    boolean isAutoGenerated = true;
    ColumnIndexCreationInfo columnIndexCreationInfo = new ColumnIndexCreationInfo(createDictionary,
        min, max, sortedArray, ForwardIndexType.FIXED_BIT_COMPRESSED,
        InvertedIndexType.SORTED_INDEX, isSortedColumn, hasNulls, totalNumberOfEntries,
        maxNumberOfMultiValueElements, fieldSpec.getDefaultNullValue(), isAutoGenerated);

    // create dictionary
    SegmentDictionaryCreator segmentDictionaryCreator =
        new SegmentDictionaryCreator(columnIndexCreationInfo.hasNulls(),
            columnIndexCreationInfo.getSortedUniqueElementsArray(), fieldSpec, tmpIndexDir);
    boolean[] isSorted = new boolean[1];
    isSorted[0] = columnIndexCreationInfo.isSorted();
    segmentDictionaryCreator.build(isSorted);
    segmentDictionaryCreator.close();

    // create forward index
    if (fieldSpec.isSingleValueField()) {

      SingleValueSortedForwardIndexCreator svFwdIndexCreator =
          new SingleValueSortedForwardIndexCreator(tmpIndexDir,
              columnIndexCreationInfo.getDistinctValueCount(), fieldSpec);
      // we will have only one value in dictionary
      int dictionaryId = 0;
      for (int docId = 0; docId < totalDocs; docId++) {
        svFwdIndexCreator.add(dictionaryId, docId);
      }
      svFwdIndexCreator.close();
    } else {
      MultiValueUnsortedForwardIndexCreator mvFwdIndexCreator =
          new MultiValueUnsortedForwardIndexCreator(fieldSpec, tmpIndexDir,
              columnIndexCreationInfo.getDistinctValueCount(), totalDocs, totalNumberOfEntries,
              hasNulls);
      // we will have only one value in dictionary
      int[] dictionaryIds = new int[] {
          0
      };
      for (int docId = 0; docId < totalDocs; docId++) {
        mvFwdIndexCreator.index(docId, dictionaryIds);
      }
      mvFwdIndexCreator.close();
    }
    // update the metadata properties with metadata for this column
    SegmentColumnarIndexCreator.addColumnMetadataInfo(metadataProperties, column,
        columnIndexCreationInfo, fieldSpec, totalDocs, totalRawDocs, totalAggDocs,
        dictionaryElementSize);

  }

  public void close() throws Exception {
    segmentDirectory.close();
  }

  public static void main(String[] args) throws Exception {
    File indexDir =
        new File("/home/kgopalak/pinot_perf/index_dir/tpch_lineitem_OFFLINE/tpch_lineitem_0");
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(indexDir);
    IndexLoadingConfigMetadata indexConfig = null;
    Schema schema = new Schema();
    schema.addField("addedField",
        new DimensionFieldSpec("addedField", FieldSpec.DataType.STRING, true));
    SchemaChangeHandler schemaChangeHandler =
        new SchemaChangeHandler(indexDir, metadata, indexConfig, schema);
    schemaChangeHandler.process();
    schemaChangeHandler.close();
  }

}
