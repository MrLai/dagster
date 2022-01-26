/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL fragment: LatestMaterializationMetadataFragment
// ====================================================

export interface LatestMaterializationMetadataFragment_runOrError_RunNotFoundError {
  __typename: "RunNotFoundError" | "PythonError";
}

export interface LatestMaterializationMetadataFragment_runOrError_Run_repositoryOrigin {
  __typename: "RepositoryOrigin";
  id: string;
  repositoryName: string;
  repositoryLocationName: string;
}

export interface LatestMaterializationMetadataFragment_runOrError_Run {
  __typename: "Run";
  id: string;
  runId: string;
  mode: string;
  pipelineName: string;
  pipelineSnapshotId: string | null;
  repositoryOrigin: LatestMaterializationMetadataFragment_runOrError_Run_repositoryOrigin | null;
}

export type LatestMaterializationMetadataFragment_runOrError = LatestMaterializationMetadataFragment_runOrError_RunNotFoundError | LatestMaterializationMetadataFragment_runOrError_Run;

export interface LatestMaterializationMetadataFragment_stepStats {
  __typename: "RunStepStats";
  endTime: number | null;
  startTime: number | null;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventTableSchemaMetadataEntry {
  __typename: "EventTableSchemaMetadataEntry" | "EventTableMetadataEntry";
  label: string;
  description: string | null;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventPythonArtifactMetadataEntry {
  __typename: "EventPythonArtifactMetadataEntry";
  label: string;
  description: string | null;
  module: string;
  name: string;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventFloatMetadataEntry {
  __typename: "EventFloatMetadataEntry";
  label: string;
  description: string | null;
  floatValue: number | null;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventIntMetadataEntry {
  __typename: "EventIntMetadataEntry";
  label: string;
  description: string | null;
  intValue: number | null;
  intRepr: string;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventPipelineRunMetadataEntry {
  __typename: "EventPipelineRunMetadataEntry";
  label: string;
  description: string | null;
  runId: string;
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventAssetMetadataEntry_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface LatestMaterializationMetadataFragment_metadataEntries_EventAssetMetadataEntry {
  __typename: "EventAssetMetadataEntry";
  label: string;
  description: string | null;
  assetKey: LatestMaterializationMetadataFragment_metadataEntries_EventAssetMetadataEntry_assetKey;
}

export type LatestMaterializationMetadataFragment_metadataEntries = LatestMaterializationMetadataFragment_metadataEntries_EventTableSchemaMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventPathMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventJsonMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventUrlMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventTextMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventMarkdownMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventPythonArtifactMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventFloatMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventIntMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventPipelineRunMetadataEntry | LatestMaterializationMetadataFragment_metadataEntries_EventAssetMetadataEntry;

export interface LatestMaterializationMetadataFragment_assetLineage_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface LatestMaterializationMetadataFragment_assetLineage {
  __typename: "AssetLineageInfo";
  assetKey: LatestMaterializationMetadataFragment_assetLineage_assetKey;
  partitions: string[];
}

export interface LatestMaterializationMetadataFragment {
  __typename: "MaterializationEvent";
  partition: string | null;
  runOrError: LatestMaterializationMetadataFragment_runOrError;
  runId: string;
  timestamp: string;
  stepKey: string | null;
  stepStats: LatestMaterializationMetadataFragment_stepStats;
  metadataEntries: LatestMaterializationMetadataFragment_metadataEntries[];
  assetLineage: LatestMaterializationMetadataFragment_assetLineage[];
}
