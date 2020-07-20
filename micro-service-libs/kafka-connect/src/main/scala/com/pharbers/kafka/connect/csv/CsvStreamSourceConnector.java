package com.pharbers.kafka.connect.csv;

import com.pharbers.kafka.connect.InputConfigKeys;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 功能描述
 *
 * @author dcs
 * @version 0.0
 * @tparam T 构造泛型参数
 * @note 一些值得注意的地方
 * @since 2019/07/02 10:26
 */
public class CsvStreamSourceConnector extends SourceConnector {

    static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(InputConfigKeys.FILE_CONFIG, Type.STRING, null, Importance.HIGH, "Source filename. If not specified, the standard input will be used")
            .define(InputConfigKeys.TOPIC_CONFIG, Type.LIST, Importance.HIGH, "The topic to publish data to")
            .define(InputConfigKeys.TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,
                    "The maximum number of records the Source task can read from file one time")
            .define(InputConfigKeys.SEPARATOR_CONFIG, Type.STRING, ",", Importance.MEDIUM, "csv分隔符, 默认为,")
            .define(InputConfigKeys.CHARSET_CONFIG, Type.STRING, "UTF-8", Importance.MEDIUM, "编码格式,大写, 默认为UTF-8")
            .define(InputConfigKeys.TITLE_CONFIG, Type.STRING, "", Importance.LOW, "csv title，使用与内容一致的分隔符分割，默认读取第一行为title")
            .define(InputConfigKeys.TRANSFORM_CONFIG, Type.STRING, "", Importance.LOW, "轻量转换");

    private String filename;
    private String topic;
    private int batchSize;
    private String separator;
    private String charset;
    private String title;
//    private List<Map<String, String>> transforms;
    private String transforms;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        filename = parsedConfig.getString(InputConfigKeys.FILE_CONFIG);
        List<String> topics = parsedConfig.getList(InputConfigKeys.TOPIC_CONFIG);
        if (topics.size() != 1) {
            throw new ConfigException("'topic' in FileStreamSourceConnector configuration requires definition of a single topic");
        }
        topic = topics.get(0);
        batchSize = parsedConfig.getInt(InputConfigKeys.TASK_BATCH_SIZE_CONFIG);
        separator = parsedConfig.getString(InputConfigKeys.SEPARATOR_CONFIG);
        charset = parsedConfig.getString(InputConfigKeys.CHARSET_CONFIG);
//        transforms = parsedConfig.getConfiguredInstances(InputConfigKeys.TITLE_CONFIG);
        transforms = parsedConfig.getString(InputConfigKeys.TITLE_CONFIG);
        if(parsedConfig.getString(InputConfigKeys.TITLE_CONFIG).equals("")){
            try(InputStream stream = Files.newInputStream(Paths.get(filename))) {
                title = new BufferedReader(new InputStreamReader(stream, Charset.forName(charset))).readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            title = parsedConfig.getString(InputConfigKeys.TITLE_CONFIG);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CsvStreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<>();
        if (filename != null)
            config.put(InputConfigKeys.FILE_CONFIG, filename);
        config.put(InputConfigKeys.TOPIC_CONFIG, topic);
        config.put(InputConfigKeys.TASK_BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        config.put(InputConfigKeys.SEPARATOR_CONFIG, separator);
        config.put(InputConfigKeys.CHARSET_CONFIG, charset);
        config.put(InputConfigKeys.TITLE_CONFIG, title);
        config.put(InputConfigKeys.TRANSFORM_CONFIG, transforms);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // 暂时无事发生
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
