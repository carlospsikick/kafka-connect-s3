package com.spredfast.kafka.connect.s3.source;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "headers",
  "offset",
  "value",
  "key",
  "timestamp"
})

public class S3ArchiveRecord {

  @JsonProperty("headers")
  private List<S3ArchiveHeader> s3ArchiveHeaders = null;
  @JsonProperty("offset")
  private Long offset;
  @JsonProperty("value")
  private String value;
  @JsonProperty("key")
  private String key;
  @JsonProperty("timestamp")
  private String timestamp;
  @JsonIgnore
  private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  @JsonProperty("headers")
  public List<S3ArchiveHeader> getHeaders() {
    return s3ArchiveHeaders;
  }

  @JsonProperty("headers")
  public void setHeaders(List<S3ArchiveHeader> s3ArchiveHeaders) {
    this.s3ArchiveHeaders = s3ArchiveHeaders;
  }

  @JsonProperty("offset")
  public Long getOffset() {
    return offset;
  }

  @JsonProperty("offset")
  public void setOffset(Long offset) {
    this.offset = offset;
  }

  @JsonProperty("value")
  public String getValue() {
    return value;
  }

  @JsonProperty("value")
  public void setValue(String value) {
    this.value = value;
  }

  @JsonProperty("key")
  public String getKey() {
    return key;
  }

  @JsonProperty("key")
  public void setKey(String key) {
    this.key = key;
  }

  @JsonProperty("timestamp")
  public String getTimestamp() {
    return timestamp;
  }

  @JsonProperty("timestamp")
  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  @JsonAnySetter
  public void setAdditionalProperty(String name, Object value) {
    this.additionalProperties.put(name, value);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(S3ArchiveRecord.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
    sb.append("headers");
    sb.append('=');
    sb.append(((this.s3ArchiveHeaders == null)?"<null>":this.s3ArchiveHeaders));
    sb.append(',');
    sb.append("offset");
    sb.append('=');
    sb.append(((this.offset == null)?"<null>":this.offset));
    sb.append(',');
    sb.append("value");
    sb.append('=');
    sb.append(((this.value == null)?"<null>":this.value));
    sb.append(',');
    sb.append("key");
    sb.append('=');
    sb.append(((this.key == null)?"<null>":this.key));
    sb.append(',');
    sb.append("timestamp");
    sb.append('=');
    sb.append(((this.timestamp == null)?"<null>":this.timestamp));
    sb.append(',');
    sb.append("additionalProperties");
    sb.append('=');
    sb.append(((this.additionalProperties == null)?"<null>":this.additionalProperties));
    sb.append(',');
    if (sb.charAt((sb.length()- 1)) == ',') {
      sb.setCharAt((sb.length()- 1), ']');
    } else {
      sb.append(']');
    }
    return sb.toString();
  }

  public static S3ArchiveRecord fromJson(byte[] jsonInput) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(jsonInput, S3ArchiveRecord.class);
  }

  public byte[] getValueB64Decoded() {
    return Base64.getDecoder().decode(this.value);
  }

  public byte[] getKeyBytes() {
    if(this.key == null){
      return null;
    }
    return Base64.getDecoder().decode(this.key);
  }
}
