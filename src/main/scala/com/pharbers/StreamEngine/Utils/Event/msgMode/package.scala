package com.pharbers.StreamEngine.Utils.Event

package object msgMode {
    case class FileMetaData(jobId: String, id: String, metaDataPath: String, sampleDataPath: String, convertType: String)
}
