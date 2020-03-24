package com.pharbers.StreamEngine.Utils.Strategy

import com.pharbers.StreamEngine.Utils.Config.BPSConfig

/** 功能描述
 *
 * @param args 构造参数
 * @tparam T 构造泛型参数
 * @author dcs
 * @version 0.0
 * @since 2020/02/04 13:49
 * @note 一些值得注意的地方
 */
trait BPSJobIdConfigStrategy{
    def getJobConfig: BPSConfig
    def getRunId: String
    def getJobId: String
}
