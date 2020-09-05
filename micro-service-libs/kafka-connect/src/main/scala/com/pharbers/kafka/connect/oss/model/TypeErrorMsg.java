package com.pharbers.kafka.connect.oss.model;

/**
 * 功能描述
 *
 * @author dcs
 * @version 0.0
 * @tparam T 构造泛型参数
 * @note 一些值得注意的地方
 * @since 2020/06/01 11:23
 */
public class TypeErrorMsg {
    private String traceId;
    private String assetId;
    private String url;
    private String type;

    public TypeErrorMsg(String traceId, String assetId, String url, String type) {
        this.traceId = traceId;
        this.assetId = assetId;
        this.url = url;
        this.type = type;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() { return url; }

    public void setUrl(String url) { this.url = url; }

}
