package com.spredfast.kafka.connect.s3.source;

import org.apache.kafka.connect.header.Header;

import java.util.List;

public class S3SourceRecord {
	private final S3Partition file;
	private final S3Offset offset;
	private final String topic;
	private final int partition;
	private final byte[] key;
	private final byte[] value;
  private final List<Header> headers;

	public S3SourceRecord(S3Partition file, S3Offset offset, String topic, int partition, byte[] key, byte[] value, List<Header> headers) {
		this.file = file;
		this.offset = offset;
		this.topic = topic;
		this.partition = partition;
		this.key = key;
		this.value = value;
    this.headers = headers;
  }

	public S3Partition file() {
		return file;
	}

	public S3Offset offset() {
		return offset;
	}

	public String topic() {
		return topic;
	}

	public int partition() {
		return partition;
	}

	public byte[] key() {
		return key;
	}

	public byte[] value() {
		return value;
	}
	public List<Header> headers(){ return headers;}
}

/*
public SourceRecord makeSourceRecord(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes) {
        final String topic = this.config.kafkaTopic;
        final Map<String, ?> sourcePartition = ImmutableMap.of(EnvelopeSchema.FIELD_ROUTINGKEY, envelope.getRoutingKey());
        final Map<String, ?> sourceOffset = ImmutableMap.of(EnvelopeSchema.FIELD_DELIVERYTAG, envelope.getDeliveryTag());

        List<Header> headers = new ArrayList<Header>();
        if (basicProperties.getHeaders() != null) {
            headers = toConnectHeaders(basicProperties.getHeaders());
        }
        Header routingKey = toConnectHeader(HEADER_KEY_ROUTING_KEY, sourcePartition.get("routingKey"));
        headers.add(routingKey);
        Header bodySchema = toConnectHeader(HEADER_KEY_BODY_SCHEMA, "BYTES_SCHEMA");
        headers.add(bodySchema);
        long timestamp = Optional.ofNullable(basicProperties.getTimestamp()).map(Date::getTime).orElse(this.time.milliseconds());
        log.info("eamessage v='{}' timestamp='{}' k='null' topic='{}' routingKey='{}'", CONFIG_VERSION, timestamp, topic, sourcePartition.get("routingKey"));


        return new SourceRecord(
          sourcePartition,
          sourceOffset,
          topic,
          null,
          null,
          null,
          BYTES_SCHEMA,
          bytes,
          timestamp,
          headers
          );
          }
 */
