/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { PipelineSelector } from "./../../../types/globalTypes";

// ====================================================
// GraphQL query operation: AssetGraphQuery
// ====================================================

export interface AssetGraphQuery_assetNodes_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetGraphQuery_assetNodes_dependencyKeys {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetGraphQuery_assetNodes_dependedByKeys {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetGraphQuery_assetNodes {
  __typename: "AssetNode";
  id: string;
  opName: string | null;
  description: string | null;
  partitionDefinition: string | null;
  assetKey: AssetGraphQuery_assetNodes_assetKey;
  dependencyKeys: AssetGraphQuery_assetNodes_dependencyKeys[];
  dependedByKeys: AssetGraphQuery_assetNodes_dependedByKeys[];
}

export interface AssetGraphQuery {
  assetNodes: AssetGraphQuery_assetNodes[];
}

export interface AssetGraphQueryVariables {
  pipelineSelector: PipelineSelector;
}
