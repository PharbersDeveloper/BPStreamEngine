package com.pharbers.StreamEngine.Utils.Module.bloodModules

case class BloodModel(parentIds: List[String],
                      mongoId: String,
                      jobId: String,
                      columnNames: List[String],
                      tabName: String,
                      length: Long,
                      url: String,
                      description: String,
                      status: String)

case class UploadEndModel(dataSetId: String, assetId: String)

case class DataMartTagModel(assetId: String, tag: String)

case class AssetDataMartModel(assetName: String,
                         assetDescription: String,
                         assetVersion: String,
                         assetDataType: String,
                         providers: List[String],
                         markets: List[String],
                         molecules: List[String],
                         dataCover: List[String],
                         geoCover: List[String],
                         labels: List[String],
                         dfs: List[String],
                         martName: String,
                         martUrl: String,
                         martDataType: String,
                         saveMode: String
                        )

case class ComplementAssetModel(providers: List[String],
                           markets: List[String],
                           molecules: List[String],
                           dataCover: List[String],
                           geoCover: List[String])
