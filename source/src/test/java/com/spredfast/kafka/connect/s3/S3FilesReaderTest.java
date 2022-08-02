package com.spredfast.kafka.connect.s3;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.spredfast.kafka.connect.s3.source.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.spredfast.kafka.connect.s3.sink.BlockGZIPFileWriter;

/**
 * Covers S3 and reading raw byte records. Closer to an integration test.
 */
public class S3FilesReaderTest {

  @Test
  public void testRecordParsing() throws IOException {
    final String value = "{\"headers\":[{\"key\":\"x-mqtt-dup\",\"value\":false},{\"key\":\"x-mqtt-publish-qos\",\"value\":1},{\"key\":\"x-shovelled-dest-uri\",\"value\":\"amqp:///nifi\"},{\"key\":\"x-shovelled-src-exchange\",\"value\":\"amq.topic\"},{\"key\":\"x-shovelled-src-exchange-key\",\"value\":\"#\"},{\"key\":\"x-shovelled-shovel-name\",\"value\":\"nifipump-ingest59\"},{\"key\":\"x-shovelled-shovelled-by\",\"value\":\"acrobatic-kingfisher\"},{\"key\":\"x-shovelled-dest-exchange\",\"value\":\"firehose.topic\"},{\"key\":\"x-shovelled-src-uri\",\"value\":\"amqp:///ingest59\"},{\"key\":\"x-shovelled-shovel-type\",\"value\":\"dynamic\"},{\"key\":\"x-shovelled-shovel-vhost\",\"value\":\"ingest59\"},{\"key\":\"x-amqp-routing-key\",\"value\":\"gz.v2.data.DK64G92\"},{\"key\":\"x-message-body-schema\",\"value\":\"BYTES_SCHEMA\"}],\"offset\":58108085,\"value\":\"Ilczc2laMkYwWlhkaGVTSTZleUp3WVdOclpYUmZiblZ0WW1WeUlqbzBOREU0TURJekxDSnljM05wWDNWd2JHbHVheUk2TFRZM0xDSnpaWEpwWVd3aU9pSkVTelkwUnpreUluMHNJbk5qYUdWdFlTSTZJbWRoZEdWM1lYbGZjMlZ1YzI5eVgyUmhkR0ZmZGpJdWMyTm9aVzFoTG1wemIyNGlMQ0p6Wlc1emIzSWlPbnNpYldGalgyRmtaSEpsYzNNaU9pSmlZem8xWlRwaE1Ub3dNRG93TURvd01EbzBPRG81TXlJc0ltMWxkSEpwWTNNaU9uc2lhSFZ0YVdScGRIa2lPalF4TGprM01qWTFOaXdpYzJOaGNDSTZOQ3dpYzNSdFgzQnliMkpsWDNOMFlYUmxJam95TENKMFpXMXdaWEpoZEhWeVpWOHdJam96TURBdU1qVXNJblJsYlhCbGNtRjBkWEpsWHpFaU9qTTFNaTR6TnpVc0luUmxiWEJsY21GMGRYSmxYeklpT2pNNE1DNHpOelVzSW5aallYQWlPak11T1RNM05YMHNJbkJoWTJ0bGRGOXVkVzFpWlhJaU9qRXlOaXdpZEd4MlgySmxjaUk2SWpVd01EWmpOREV5TURZeE5tTTJNVGMxTVRBeVpqa3lPVFUyTURFd01qVm1NekF3TVRObU5XWXpNVEF4TkRBaUxDSjFibk52YkdsamFYUmxaQ0k2Wm1Gc2MyVjlMQ0owYVcxbGMzUmhiWEFpT2lJeU1ESXlMVEEzTFRJNFZEQXdPakF4T2pVNVdpSjlYUT09Ig==\",\"key\":null,\"timestamp\":\"2022-07-28T00:01:59.200Z\"}\n";
    /*ObjectMapper mapper = new ObjectMapper();
    S3ArchiveRecord record = mapper.readValue(value, S3ArchiveRecord.class);*/
    S3ArchiveRecord record = S3ArchiveRecord.fromJson(value.getBytes(StandardCharsets.UTF_8));
    assertNotNull(record);
    assertEquals("2022-07-28T00:01:59.200Z", record.getTimestamp());
    assertNull(record.getKey());
    String val = record.getValue();
    assertEquals( "Ilczc2laMkYwWlhkaGVTSTZleUp3WVdOclpYUmZiblZ0WW1WeUlqbzBOREU0TURJekxDSnljM05wWDNWd2JHbHVheUk2TFRZM0xDSnpaWEpwWVd3aU9pSkVTelkwUnpreUluMHNJbk5qYUdWdFlTSTZJbWRoZEdWM1lYbGZjMlZ1YzI5eVgyUmhkR0ZmZGpJdWMyTm9aVzFoTG1wemIyNGlMQ0p6Wlc1emIzSWlPbnNpYldGalgyRmtaSEpsYzNNaU9pSmlZem8xWlRwaE1Ub3dNRG93TURvd01EbzBPRG81TXlJc0ltMWxkSEpwWTNNaU9uc2lhSFZ0YVdScGRIa2lPalF4TGprM01qWTFOaXdpYzJOaGNDSTZOQ3dpYzNSdFgzQnliMkpsWDNOMFlYUmxJam95TENKMFpXMXdaWEpoZEhWeVpWOHdJam96TURBdU1qVXNJblJsYlhCbGNtRjBkWEpsWHpFaU9qTTFNaTR6TnpVc0luUmxiWEJsY21GMGRYSmxYeklpT2pNNE1DNHpOelVzSW5aallYQWlPak11T1RNM05YMHNJbkJoWTJ0bGRGOXVkVzFpWlhJaU9qRXlOaXdpZEd4MlgySmxjaUk2SWpVd01EWmpOREV5TURZeE5tTTJNVGMxTVRBeVpqa3lPVFUyTURFd01qVm1NekF3TVRObU5XWXpNVEF4TkRBaUxDSjFibk52YkdsamFYUmxaQ0k2Wm1Gc2MyVjlMQ0owYVcxbGMzUmhiWEFpT2lJeU1ESXlMVEEzTFRJNFZEQXdPakF4T2pVNVdpSjlYUT09Ig==",
      val);
    List<S3ArchiveHeader> s3ArchiveHeaders = record.getHeaders();
    for (S3ArchiveHeader h : s3ArchiveHeaders) {
      if (h.getKey().equals("x-shovelled-src-exchange")) {
        assertEquals("amq.topic", h.getValue());
      }
    }
  }

	@Test
	public void testReadingBytesFromS3() throws IOException {
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeData(dir);

		final AmazonS3 client = givenAMockS3Client(dir);

		List<String> results = whenTheRecordsAreRead(client, true, 3);

		thenTheyAreFilteredAndInOrder(results);
	}


	@Test
	public void testReadingBytesFromS3_multiPartition() throws IOException {
		// scenario: multiple partition files at the end of a listing, page size >  # of files
		// do we read all of them?
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenASingleDayWithManyPartitions(dir);

		final AmazonS3 client = givenAMockS3Client(dir);

		List<String> results = whenTheRecordsAreRead(client, true, 10);

		thenTheyAreFilteredAndInOrder(results);
	}

	@Test
	public void testReadingBytesFromS3_withOffsets() throws IOException {
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeData(dir);

		final AmazonS3 client = givenAMockS3Client(dir);

		List<String> results = whenTheRecordsAreRead(givenAReaderWithOffsets(client,
			"prefix/20151231/topic-00000000-00003-000000000001.gz", 5L, "00003"));

		assertEquals(Arrays.asList(
			"willbe=skipped5",
			"willbe=skipped6",
			"willbe=skipped7",
			"willbe=skipped8",
			"willbe=skipped9"
		), results);
	}

  @Test
  public void testParseKeyPattern() throws IOException {
	  String fileKey="2022/20220101/firehose-20220711-3-54386117.gz";
    Pattern keyPattern = S3FilesReader.DEFAULT_PATTERN;

    final Matcher matcher = keyPattern.matcher(fileKey);
    boolean found = matcher.find();
    assertTrue(found);
    final String topic = matcher.group("topic");
    assertEquals("firehose", topic);
    final String timestamp = matcher.group("timestamp");
    assertEquals("20220711", timestamp);
    final int partition = Integer.parseInt(matcher.group("partition"));
    assertEquals(3, partition);
    final long startOffset = Long.parseLong(matcher.group("offset"));
    assertEquals(54386117, startOffset);

    Pattern DATA_SUFFIX = Pattern.compile("\\.gz$");
    String result = DATA_SUFFIX.matcher(fileKey).replaceAll(".index.json");
    assertEquals(fileKey, result);

  }

	@Test
	public void testReadingBytesFromS3_withOffsetsAtEndOfFile() throws IOException {
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeData(dir);

		final AmazonS3 client = givenAMockS3Client(dir);

		// this file will be skipped
		List<String> results = whenTheRecordsAreRead(givenAReaderWithOffsets(client,
			"prefix/20151230/topic-00000000-00003-000000000000.gz", 1L, "00003"));

		assertEquals(Arrays.asList(
			"willbe=skipped1",
			"willbe=skipped2",
			"willbe=skipped3",
			"willbe=skipped4",
			"willbe=skipped5",
			"willbe=skipped6",
			"willbe=skipped7",
			"willbe=skipped8",
			"willbe=skipped9"
		), results);
	}

	S3FilesReader givenAReaderWithOffsets(AmazonS3 client, String marker, long nextOffset, final String partition) {
		Map<S3Partition, S3Offset> offsets = new HashMap<>();
		int partInt = Integer.valueOf(partition, 10);
		offsets.put(S3Partition.from("bucket", "prefix", "topic", partInt),
			S3Offset.from(marker, nextOffset - 1 /* an S3 offset is the last record processed, so go back 1 to consume next */));
		return new S3FilesReader(new S3SourceConfig("bucket", "prefix", 1, null, S3FilesReader.DEFAULT_PATTERN, S3FilesReader.InputFilter.GUNZIP,
			p -> partInt == p), client, offsets, () -> new BytesRecordReader(true));
	}

	public static class ReversedStringBytesConverter implements Converter {
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			// while we're here, verify that we get our subconfig
			assertEquals(configs.get("requiredProp"), "isPresent");
		}

		@Override
		public byte[] fromConnectData(String topic, Schema schema, Object value) {
			byte[] bytes = value.toString().getBytes(Charset.forName("UTF-8"));
			byte[] result = new byte[bytes.length];
			for (int i = 0; i < bytes.length; i++) {
				result[bytes.length - i - 1] = bytes[i];
			}
			return result;
		}

		@Override
		public SchemaAndValue toConnectData(String topic, byte[] value) {
			throw new UnsupportedOperationException();
		}
	}

	@Test
	public void testReadingBytesFromS3_withoutKeys() throws IOException {
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeData(dir, false);

		final AmazonS3 client = givenAMockS3Client(dir);

		List<String> results = whenTheRecordsAreRead(client, false);

		theTheyAreInOrder(results);
	}

	Converter givenACustomConverter() {
		Map<String, Object> config = new HashMap<>();
		config.put("converter", AlreadyBytesConverter.class.getName());
		config.put("converter.converter", ReversedStringBytesConverter.class.getName());
		config.put("converter.converter.requiredProp", "isPresent");
		return Configure.buildConverter(config, "converter", false, null);
	}

	void theTheyAreInOrder(List<String> results) {
		List<String> expected = Arrays.asList(
			"value0-0",
			"value1-0",
			"value1-1"
		);
		assertEquals(expected, results);
	}

	private void thenTheyAreFilteredAndInOrder(List<String> results) {
		List<String> expected = Arrays.asList(
			"key0-0=value0-0",
			"key1-0=value1-0",
			"key1-1=value1-1"
		);
		assertEquals(expected, results);
	}

	private List<String> whenTheRecordsAreRead(AmazonS3 client, boolean fileIncludesKeys) {
		return whenTheRecordsAreRead(client, fileIncludesKeys, 1);
	}

	private List<String> whenTheRecordsAreRead(AmazonS3 client, boolean fileIncludesKeys, int pageSize) {
		S3FilesReader reader = new S3FilesReader(new S3SourceConfig("bucket", "prefix", pageSize, "prefix/2016-01-01", S3FilesReader.DEFAULT_PATTERN, S3FilesReader.InputFilter.GUNZIP, null), client, null,() -> new BytesRecordReader(fileIncludesKeys));
		return whenTheRecordsAreRead(reader);
	}

	private List<String> whenTheRecordsAreRead(S3FilesReader reader) {
		List<String> results = new ArrayList<>();
		for (S3SourceRecord record : reader) {
			results.add((record.key() == null ? "" : new String(record.key()) + "=") + new String(record.value()));
		}
		return results;
	}

	private AmazonS3 givenAMockS3Client(final Path dir) {
		final HashMap<String, String> config = new HashMap<>();
		config.put(S3ConfigurationConfig.S3_BUCKET_CONFIG, "test");
		config.put(S3ConfigurationConfig.S3_ENDPOINT_URL_CONFIG, "s3.my-region.mock.com");

		final AmazonS3 client = spy(S3.s3client(config));

		doAnswer(new Answer<ObjectListing>() {
			@Override
			public ObjectListing answer(InvocationOnMock invocationOnMock) throws Throwable {
				final ListObjectsRequest req = (ListObjectsRequest) invocationOnMock.getArguments()[0];
				ObjectListing listing = new ObjectListing();

				final Set<File> files = new TreeSet<>();
				Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult preVisitDirectory(Path toCheck, BasicFileAttributes attrs) throws IOException {
						if (toCheck.startsWith(dir)) {
							return FileVisitResult.CONTINUE;
						}
						return FileVisitResult.SKIP_SUBTREE;
					}

					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						String key = key(file.toFile());
						if (req.getMarker() == null
							|| key.compareTo(req.getMarker()) > 0) {
							files.add(file.toFile());
						}
						return FileVisitResult.CONTINUE;
					}
				});

				List<S3ObjectSummary> summaries = new ArrayList<>();
				int count = 0;
				for (File file : files) {
					if (count++ < req.getMaxKeys()) {
						S3ObjectSummary summary = new S3ObjectSummary();
						String key = key(file);
						summary.setKey(key);
						listing.setNextMarker(key);
						summaries.add(summary);
					} else {
						break;
					}
				}

				listing.setMaxKeys(req.getMaxKeys());

				listing.getObjectSummaries().addAll(summaries);
				listing.setTruncated(files.size() > req.getMaxKeys());

				return listing;
			}

			private String key(File file) {
				return file.getAbsolutePath().substring(dir.toAbsolutePath().toString().length() + 1);
			}
		}).when(client).listObjects(any(ListObjectsRequest.class));


		doCallRealMethod().when(client).listNextBatchOfObjects(any(ObjectListing.class));
		doCallRealMethod().when(client).listNextBatchOfObjects(any(ListNextBatchOfObjectsRequest.class));

		doAnswer((Answer<S3Object>) invocationOnMock -> {
			String key = (String) invocationOnMock.getArguments()[1];
			return getFile(key, dir);
		}).when(client).getObject(anyString(), anyString());


		doAnswer((Answer<S3Object>) invocationOnMock -> {
			String key = ((GetObjectRequest) invocationOnMock.getArguments()[0]).getKey();
			return getFile(key, dir);
		}).when(client).getObject(any(GetObjectRequest.class));

		doAnswer((Answer<S3Object>) invocationOnMock -> {
			String key = (String) invocationOnMock.getArguments()[1];
			if (!new File(dir.toString(), key).exists()) {
				AmazonServiceException e = new AmazonServiceException("Nope: " + key);
				e.setErrorCode("NoSuchKey");
				throw e;
			}
			return null;
		}).when(client).getObjectMetadata(anyString(), anyString());

		return client;
	}

	S3Object getFile(String key, Path dir) throws FileNotFoundException {
		S3Object obj = mock(S3Object.class);
		File file = new File(dir.toString(), key);
		when(obj.getKey()).thenReturn(file.getName());
		S3ObjectInputStream stream = new S3ObjectInputStream(new FileInputStream(file), null);
		when(obj.getObjectContent()).thenReturn(stream);
		return obj;
	}

	private void givenASingleDayWithManyPartitions(Path dir) throws IOException {
		givenASingleDayWithManyPartitions(dir, true);
	}

	private void givenASingleDayWithManyPartitions(Path dir, boolean includeKeys) throws IOException {
		new File(dir.toFile(), "prefix/20160101").mkdirs();
		try (BlockGZIPFileWriter p0 = new BlockGZIPFileWriter("topic-00000000-00000", dir.toString() + "/prefix/20160101", 0, 512);
			 BlockGZIPFileWriter p1 = new BlockGZIPFileWriter("topic-00000000-00001", dir.toString() + "/prefix/20160101", 0, 512);
		) {
			write(p0, "key0-0".getBytes(), "value0-0".getBytes(), includeKeys);
			write(p1, "key1-0".getBytes(), "value1-0".getBytes(), includeKeys);
			write(p1, "key1-1".getBytes(), "value1-1".getBytes(), includeKeys);
		}
	}

	private void givenSomeData(Path dir) throws IOException {
		givenSomeData(dir, true);
	}

	private void givenSomeData(Path dir, boolean includeKeys) throws IOException {
		new File(dir.toFile(), "prefix/20151230").mkdirs();
		new File(dir.toFile(), "prefix/20151231").mkdirs();
		new File(dir.toFile(), "prefix/20160101").mkdirs();
		new File(dir.toFile(), "prefix/20160102").mkdirs();
		try (BlockGZIPFileWriter writer0 = new BlockGZIPFileWriter("topic-00000000-00003", dir.toString() + "/prefix/20151231", 1, 512);
			 BlockGZIPFileWriter writer1 = new BlockGZIPFileWriter("topic-00000000-00000", dir.toString() + "/prefix/20160101", 0, 512);
			 BlockGZIPFileWriter writer2 = new BlockGZIPFileWriter("topic-00000000-00001", dir.toString() + "/prefix/20160102", 0, 512);
			 BlockGZIPFileWriter preWriter1 = new BlockGZIPFileWriter("topic-00000000-00003", dir.toString() + "/prefix/20151230", 0, 512);
		) {
			write(preWriter1, "willbe".getBytes(), "skipped0".getBytes(), includeKeys);

			for (int i = 1; i < 10; i++) {
				write(writer0, "willbe".getBytes(), ("skipped" + i).getBytes(), includeKeys);
			}

			write(writer1, "key0-0".getBytes(), "value0-0".getBytes(), includeKeys);

			write(writer2, "key1-0".getBytes(), "value1-0".getBytes(), includeKeys);
			write(writer2, "key1-1".getBytes(), "value1-1".getBytes(), includeKeys);
		}
	}

	private void write(BlockGZIPFileWriter writer, byte[] key, byte[] value, boolean includeKeys) throws IOException {
		writer.write(new ByteLengthFormat(includeKeys).newWriter().writeBatch(Stream.of(new ProducerRecord<>("", key, value))).collect(toList()), 1);
	}


}
