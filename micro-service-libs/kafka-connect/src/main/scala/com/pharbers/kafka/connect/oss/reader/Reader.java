package com.pharbers.kafka.connect.oss.reader;

import com.pharbers.kafka.schema.OssTask;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * 功能描述
 *
 * @author dcs
 * @version 0.0
 * @tparam T 构造泛型参数
 * @note 一些值得注意的地方
 * @since 2019/10/28 18:28
 */
public interface Reader {
    List<SourceRecord> read() throws Exception;
    void init(InputStream stream, OssTask task, Map<String, Object> streamOffset) throws Exception;
    boolean isEnd();
    void close();
}
