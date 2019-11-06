package com.pharbers.StreamEngine.Utils.Config

import java.util
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/** kafka.AbstractConfig 的 BPS 实现
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/06 11:03
 * @note originals 中的值会覆盖 definition 中定义的默认值
 */
class BPSConfig(definition: ConfigDef, originals: util.Map[_, _]) extends AbstractConfig(definition, originals)

