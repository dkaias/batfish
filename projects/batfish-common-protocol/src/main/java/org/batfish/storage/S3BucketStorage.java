package org.batfish.storage;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.batfish.common.BfConsts.RELPATH_INPUT;
import static org.batfish.common.BfConsts.RELPATH_ISP_CONFIG_FILE;
import static org.batfish.common.plugin.PluginConsumer.DEFAULT_HEADER_LENGTH_BYTES;
import static org.batfish.common.plugin.PluginConsumer.detectFormat;
import static org.batfish.common.util.Resources.readFile;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.Closer;
import com.google.errorprone.annotations.MustBeClosed;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.batfish.common.BatfishException;
import org.batfish.common.BatfishLogger;
import org.batfish.common.BfConsts;
import org.batfish.common.CompletionMetadata;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.runtime.SnapshotRuntimeData;
import org.batfish.common.topology.L3Adjacencies;
import org.batfish.common.topology.Layer1Topology;
import org.batfish.common.util.BatfishObjectMapper;
import org.batfish.common.plugin.PluginConsumer;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.SnapshotMetadata;
import org.batfish.datamodel.Topology;
import org.batfish.datamodel.answers.AnswerMetadata;
import org.batfish.datamodel.answers.ConvertConfigurationAnswerElement;
import org.batfish.datamodel.answers.ParseEnvironmentBgpTablesAnswerElement;
import org.batfish.datamodel.answers.ParseVendorConfigurationAnswerElement;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.datamodel.collections.BgpAdvertisementsByVrf;
import org.batfish.datamodel.collections.NodeInterfacePair;
import org.batfish.datamodel.eigrp.EigrpTopology;
import org.batfish.datamodel.isp_configuration.IspConfiguration;
import org.batfish.datamodel.isp_configuration.IspConfigurationException;
import org.batfish.datamodel.ospf.OspfTopology;
import org.batfish.datamodel.questions.Question;
import org.batfish.datamodel.vxlan.VxlanTopology;
import org.batfish.identifiers.AnswerId;
import org.batfish.identifiers.Id;
import org.batfish.identifiers.NetworkId;
import org.batfish.identifiers.NodeRolesId;
import org.batfish.identifiers.QuestionId;
import org.batfish.identifiers.SnapshotId;
import org.batfish.referencelibrary.ReferenceLibrary;
import org.batfish.role.NodeRolesData;
import org.batfish.vendor.ConversionContext;
import org.batfish.vendor.VendorConfiguration;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;

/** A utility class that abstracts the underlying file system storage used by Batfish. */
@ParametersAreNonnullByDefault
public class S3BucketStorage implements StorageProvider {
  private static final Logger LOGGER = LogManager.getLogger(S3BucketStorage.class);

  @VisibleForTesting static final Duration GC_SKEW_ALLOWANCE = Duration.ofMinutes(10L);
  private static final String ID_EXTENSION = ".id";
  private static final String SUFFIX_LOG_FILE = ".log";
  private static final String SUFFIX_ANSWER_JSON_FILE = ".json";
  private static final String RELPATH_COMPLETION_METADATA_FILE = "completion_metadata.json";
  private static final String RELPATH_BGP_TOPOLOGY = "bgp_topology.json";
  private static final String RELPATH_EIGRP_TOPOLOGY = "eigrp_topology.json";
  private static final String RELPATH_SYNTHESIZED_LAYER1_TOPOLOGY =
          "synthesized_layer1_topology.json";
  private static final String RELPATH_LAYER3_TOPOLOGY = "layer3_topology.json";
  private static final String RELPATH_L3_ADJACENCIES = "l3_adjacencies";
  private static final String RELPATH_OSPF_TOPOLOGY = "ospf_topology.json";
  private static final String RELPATH_VXLAN_TOPOLOGY = "vxlan_topology.json";
  private static final String RELPATH_VENDOR_INDEPENDENT_CONFIG_DIR = "indep";
  private static final String RELPATH_QUESTIONS_DIR = "questions";
  private static final String RELPATH_TESTRIG_POJO_TOPOLOGY_PATH = "testrig_pojo_topology";
  private static final String RELPATH_ORIGINAL_DIR = "original";
  private static final String RELPATH_METADATA_FILE = "metadata.json";
  private static final String RELPATH_FORK_REQUEST_FILE = "fork_request";
  private static final String RELPATH_ENV_TOPOLOGY_FILE = "env_topology";
  private static final String RELPATH_CONVERSION_CONTEXT = "conversion_context";
  private static final String RELPATH_CONVERT_ANSWER_PATH = "convert_answer";
  private static final String RELPATH_ANSWERS_DIR = "answers";
  private static final String RELPATH_ANSWER_METADATA = "answer_metadata.json";
  private static final String RELPATH_ANSWER_JSON = "answer.json";
  private static final String RELPATH_ANALYSES_DIR = "analyses";
  private static final String RELPATH_BATFISH_CONFIGS_DIR = "batfish";
  private static final String RELPATH_SNAPSHOT_ZIP_FILE = "snapshot.zip";
  private static final String RELPATH_DATA_PLANE = "dp";
  private static final String RELPATH_SERIALIZED_ENVIRONMENT_BGP_TABLES = "bgp_processed";
  private static final String RELPATH_ENVIRONMENT_BGP_TABLES_ANSWER = "bgp_answer";
  private static final String RELPATH_PARSE_ANSWER_PATH = "parse_answer";
  private static final String RELPATH_VENDOR_SPECIFIC_CONFIG_DIR = "vendor";
  private static final String RELPATH_AWS_ACCOUNTS_DIR = "accounts";
  private static final String RELPATH_SNAPSHOTS_DIR = "snapshots";
  private static final String RELPATH_OUTPUT = "output";
  private static final String FOLDER_SUFFIX = "/";

  private final BatfishLogger _logger;
  private final BiFunction<String, Integer, AtomicInteger> _newBatch;
  private final Path _baseDir;
  private static S3BucketConfiguration _s3cfg;
  private final S3Client _s3Client;

  /**
   * Create a new {@link S3BucketStorage} instance that uses the given root path and job batch
   * provider function.
   */
  public S3BucketStorage(String configFile, Path baseDir, BatfishLogger logger) {
    _logger = logger;
    _baseDir = baseDir;
    _newBatch = (a, b) -> new AtomicInteger();
    // TODO: how to handle file not existing?
    try {
      _s3cfg = BatfishObjectMapper
              .mapper()
              .readValue(readFile(configFile, UTF_8), S3BucketConfiguration.class);
    } catch (IOException e) {
      if (_logger != null) {
        _logger.warn("Unable to load s3 config file " + configFile + " .\n");
      }
    }

    _s3Client = S3Client.builder()
            .region(Region.of(_s3cfg.getRegionName()))
            .credentialsProvider(ProfileCredentialsProvider.create(_s3cfg.getProfileName()))
            .build();
  }

  private boolean objectExists(String objectPath) {
    HeadObjectRequest objRequest = HeadObjectRequest.builder()
            .bucket(_s3cfg.getBucketName())
            .key(objectPath)
            .build();
    try {
      _s3Client.headObject(objRequest);
    } catch (NoSuchKeyException e) {
      return false;
    }
    return true;
  }

  private void createFolder(Path dir) throws IOException {
    String folderName = toKeyPath(dir) + FOLDER_SUFFIX;
    PutObjectRequest putRequest = PutObjectRequest.builder()
            .bucket(_s3cfg.getBucketName())
            .key(folderName)
            .build();
    _s3Client.putObject(putRequest, RequestBody.empty());

    _logger.info("Created folder: " + folderName + "\n");
  }

  private String toKeyPath(Path path) {
    String pathAsString = path.toString();
    return pathAsString.startsWith("/") ? pathAsString.substring(1) : pathAsString;
  }

  private Path fromKeyPath(String keyPath) {
    if (!keyPath.startsWith("/")) {
      keyPath = "/" + keyPath;
    }
    return Path.of(keyPath);
  }

  private String toFolderPath(Path path) {
    String folderPath = toKeyPath(path);
    return folderPath.endsWith("/") ? folderPath : folderPath + "/";
  }

  private ResponseInputStream<GetObjectResponse> getObjectStream(String objKey) throws NoSuchKeyException {
    GetObjectRequest objRequest = GetObjectRequest.builder()
            .bucket(_s3cfg.getBucketName())
            .key(objKey)
            .build();
    return _s3Client.getObject(objRequest);
  }

  // TODO: do we want a delimiter for this?
  private Stream<S3Object> listFolder(Path dir) {
    String prefix = toFolderPath(dir);
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
            .bucket(_s3cfg.getBucketName())
            .prefix(prefix)
            //.delimiter("/")
            .build();
    ListObjectsV2Iterable listResponse = _s3Client.listObjectsV2Paginator(listRequest);
    return listResponse.contents().stream();
  }

  // helper method to return common prefixes under any folder. S3 does not have a concept of directories,
  // folder boundaries are represented as "common prefixes" which are not objects. This will list all
  // folders within a particular folder, not it's objects.
  private List<String> listCommonPrefixes(Path path) {
    String prefix = toFolderPath(path);
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
            .bucket(_s3cfg.getBucketName())
            .prefix(prefix)
            .delimiter("/")
            .build();
    ListObjectsV2Iterable listResponse = _s3Client.listObjectsV2Paginator(listRequest);
    return listResponse.commonPrefixes().stream()
            .map(commonPrefix -> commonPrefix.prefix())
            .collect(Collectors.toList());
  }

  private void deleteDirectory(Path path) throws IOException {
    Path sanitizedPath = validatePath(path);
    deleteObjects(sanitizedPath);
  }

  private boolean deleteObjects(Path path) throws SdkException {
    List<ObjectIdentifier> keysToDelete;
    if (isDirectory(path)) {
      Stream<S3Object> objStream = listFolder(path);
      keysToDelete = objStream
              .map(obj -> obj.key())
              .map(obj -> ObjectIdentifier.builder().key(obj).build())
              .collect(Collectors.toList());
    } else {
      keysToDelete = new ArrayList<>();
      ObjectIdentifier objToDelete = ObjectIdentifier.builder()
              .key(toKeyPath(path))
              .build();
      keysToDelete.add(objToDelete);
    }

    DeleteObjectsRequest delRequest = DeleteObjectsRequest.builder()
            .bucket(_s3cfg.getBucketName())
            .delete(Delete.builder().objects(keysToDelete).build())
            .build();

    DeleteObjectsResponse delResponse = _s3Client.deleteObjects(delRequest);
    boolean allDeleted = delResponse.deleted().size() == keysToDelete.size();
    return allDeleted;
  }

  private boolean isDirectory(Path path) {
    Path sanitizedPath = validatePath(path);
    String folderKey = toFolderPath(sanitizedPath);

    // we can determine if this is a folder by looking at the key count on the response object. it does not matter
    // if there is a slash suffix
    // 0 - folder does not exist
    // 1 - folder exists but is empty
    // >1 - folder exists with child objects
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
            .bucket(_s3cfg.getBucketName())
            .prefix(folderKey)
            .build();

    ListObjectsV2Response listResponse = _s3Client.listObjectsV2(listRequest);
    return listResponse.keyCount() > 0;
  }


  private String relativizeKeyPath(Path inputPath, String keyPath) {
    String inputPathAsString = toKeyPath(inputPath);
    String[] components = keyPath.split(inputPathAsString);
    return StringUtils.stripStart(components[1], "/");
  }

  /**
   * Returns the configuration files for the given testrig. If a serialized copy of these
   * configurations is not already present, then this function returns {@code null}.
   */
  @Override
  @Nullable
  public SortedMap<String, Configuration> loadConfigurations(
          NetworkId network, SnapshotId snapshot) {
    Path indepDir = getVendorIndependentConfigDir(network, snapshot);
    // If the directory that would contain these configs does not even exist, no cache exists.
    if (!isDirectory(indepDir)) {
      _logger.debugf("Unable to load configs for %s from disk: no cache directory", snapshot);
      return null;
    }

    // If the directory exists, then likely the configs exist and are useful. Still, we need to
    // confirm that they were serialized with a compatible version of Batfish first.
    if (!cachedConfigsAreCompatible(network, snapshot)) {
      _logger.debugf(
              "Unable to load configs for %s from disk: error or incompatible version", snapshot);
      return null;
    }

    _logger.info("\n*** DESERIALIZING VENDOR-INDEPENDENT CONFIGURATION STRUCTURES ***\n");
    Map<Path, String> namesByPath = new TreeMap<>();
    try (Stream<S3Object> stream = listFolder(indepDir)) {
      stream.forEach(obj -> {
        String name = StringUtils.substringAfterLast(obj.key(), "/");
        namesByPath.put(fromKeyPath(obj.key()), name);
      });
    }

    try {
      return deserializeObjects(namesByPath, Configuration.class);
    } catch (BatfishException e) {
      return null;
    }
  }

  @Override
  public @Nonnull ConversionContext loadConversionContext(NetworkSnapshot snapshot)
          throws IOException {
    Path ccPath = getConversionContextPath(snapshot.getNetwork(), snapshot.getSnapshot());
    if (!objectExists(toKeyPath(ccPath))) {
      throw new FileNotFoundException();
    }
    try {
      return deserializeObject(ccPath, ConversionContext.class);
    } catch (BatfishException e) {
      throw new IOException(
              String.format(
                      "Failed to deserialize ConversionContext: %s", Throwables.getStackTraceAsString(e)));
    }
  }

  @Override
  public @Nullable ConvertConfigurationAnswerElement loadConvertConfigurationAnswerElement(
          NetworkId network, SnapshotId snapshot) {
    Path ccaePath = getConvertAnswerPath(network, snapshot);
    String ccaeKey = toKeyPath(ccaePath);
    if (!objectExists(ccaeKey)) {
      return null;
    }
    try {
      return deserializeObject(ccaePath, ConvertConfigurationAnswerElement.class);
    } catch (BatfishException e) {
      _logger.errorf(
              "Failed to deserialize ConvertConfigurationAnswerElement: %s",
              Throwables.getStackTraceAsString(e));
      LOGGER.error("Failed to deserialize ConvertConfigurationAnswerElement", e);
      return null;
    }
  }

  @Override
  public @Nullable SortedSet<NodeInterfacePair> loadInterfaceBlacklist(
          NetworkId network, SnapshotId snapshot) {
    // Prefer runtime data inside of batfish/ subfolder over top level
    Path insideBatfish =
            Paths.get(
                    RELPATH_INPUT, RELPATH_BATFISH_CONFIGS_DIR, BfConsts.RELPATH_INTERFACE_BLACKLIST_FILE);
    Path topLevel = Paths.get(RELPATH_INPUT, BfConsts.RELPATH_INTERFACE_BLACKLIST_FILE);
    Optional<Path> path =
            Stream.of(insideBatfish, topLevel)
                    .map(p -> getSnapshotDir(network, snapshot).resolve(p))
                    .filter(p -> objectExists(toKeyPath(p)))
                    .findFirst();
    if (!path.isPresent()) {
      // Neither file was present in input.
      return null;
    }

    try {
      InputStream objStream = getObjectStream(toKeyPath(path.get()));
      return BatfishObjectMapper.mapper()
              .readValue(objStream, new TypeReference<SortedSet<NodeInterfacePair>>() {});
    } catch (IOException e) {
      _logger.warnf(
              "Unexpected exception caught while loading interface blacklist for snapshot %s: %s",
              snapshot, Throwables.getStackTraceAsString(e));
      LOGGER.warn(
              String.format(
                      "Unexpected exception caught while loading interface blacklist for snapshot %s",
                      snapshot),
              e);
      return null;
    }
  }

  @VisibleForTesting
  static final String ISP_CONFIGURATION_KEY =
          String.format("%s/%s", RELPATH_BATFISH_CONFIGS_DIR, RELPATH_ISP_CONFIG_FILE);

  @VisibleForTesting
  static boolean keyInDir(String key, String dirName) {
    return key.startsWith(dirName + "/");
  }

  @Override
  public @Nullable IspConfiguration loadIspConfiguration(NetworkId network, SnapshotId snapshot)
          throws IspConfigurationException {
    try (InputStream inputStream =
                 loadSnapshotInputObject(network, snapshot, ISP_CONFIGURATION_KEY)) {
      return BatfishObjectMapper.mapper().readValue(inputStream, IspConfiguration.class);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      throw new IspConfigurationException(
              String.format(
                      "Could not parse the content of %s. (Is it valid JSON? Does it have the right"
                              + " information?): %s",
                      ISP_CONFIGURATION_KEY, e.getMessage()),
              e);
    }
  }

  @Override
  public @Nullable SortedSet<String> loadNodeBlacklist(NetworkId network, SnapshotId snapshot) {
    // Prefer runtime data inside of batfish/ subfolder over top level
    Path insideBatfish =
            Paths.get(RELPATH_INPUT, RELPATH_BATFISH_CONFIGS_DIR, BfConsts.RELPATH_NODE_BLACKLIST_FILE);
    Path topLevel = Paths.get(RELPATH_INPUT, BfConsts.RELPATH_NODE_BLACKLIST_FILE);
    Optional<Path> path =
            Stream.of(insideBatfish, topLevel)
                    .map(p -> getSnapshotDir(network, snapshot).resolve(p))
                    .filter(p -> objectExists(toKeyPath(p)))
                    .findFirst();
    if (!path.isPresent()) {
      // Neither file was present in input.
      return null;
    }

    try {
      InputStream objStream = getObjectStream(toKeyPath(path.get()));
      return BatfishObjectMapper.mapper()
              .readValue(objStream, new TypeReference<SortedSet<String>>() {});
    } catch (IOException e) {
      _logger.warnf(
              "Unexpected exception caught while loading node blacklist for snapshot %s: %s",
              snapshot, Throwables.getStackTraceAsString(e));
      LOGGER.warn(
              String.format(
                      "Unexpected exception caught while loading node blacklist for snapshot %s", snapshot),
              e);
      return null;
    }
  }

  @Override
  public @Nullable Layer1Topology loadLayer1Topology(NetworkId network, SnapshotId snapshot) {
    // Prefer runtime data inside of batfish/ subfolder over top level
    Path insideBatfish =
            Paths.get(RELPATH_INPUT, RELPATH_BATFISH_CONFIGS_DIR, BfConsts.RELPATH_L1_TOPOLOGY_PATH);
    Path topLevel = Paths.get(RELPATH_INPUT, BfConsts.RELPATH_L1_TOPOLOGY_PATH);
    Path deprecated = Paths.get(RELPATH_INPUT, "testrig_layer1_topology");
    Optional<Path> path =
            Stream.of(insideBatfish, topLevel, deprecated)
                    .map(p -> getSnapshotDir(network, snapshot).resolve(p))
                    .filter(p -> objectExists(toKeyPath(p)))
                    .findFirst();
    if (!path.isPresent()) {
      // Neither file was present in input.
      return null;
    }

    AtomicInteger counter = _newBatch.apply("Reading layer-1 topology", 1);
    try {
      InputStream objStream = getObjectStream(toKeyPath(path.get()));
      return BatfishObjectMapper.mapper().readValue(objStream, Layer1Topology.class);
    } catch (IOException e) {
      _logger.warnf(
              "Unexpected exception caught while loading layer-1 topology for snapshot %s: %s",
              snapshot, Throwables.getStackTraceAsString(e));
      LOGGER.warn(
              String.format(
                      "Unexpected exception caught while loading layer-1 topology for snapshot %s",
                      snapshot),
              e);
      return null;
    } finally {
      counter.incrementAndGet();
    }
  }

  @Override
  @Nonnull
  public String loadWorkLog(NetworkId network, SnapshotId snapshot, String workId)
          throws IOException {
    Path filePath = getWorkLogPath(network, snapshot, workId);
    return readFileToString(filePath, UTF_8);
  }

  @Override
  @Nonnull
  public String loadWorkJson(NetworkId network, SnapshotId snapshot, String workId)
          throws IOException {
    Path filePath = getWorkJsonPath(network, snapshot, workId);
    if (!objectExists(toKeyPath(filePath))) {
      throw new FileNotFoundException(
              String.format("Could not find work json for work ID: %s", workId));
    }
    return readFileToString(filePath, UTF_8);
  }

  @Override
  public @Nullable SnapshotRuntimeData loadRuntimeData(NetworkId network, SnapshotId snapshot) {
    Path path =
            getSnapshotDir(network, snapshot)
                    .resolve(
                            Paths.get(
                                    RELPATH_INPUT,
                                    RELPATH_BATFISH_CONFIGS_DIR,
                                    BfConsts.RELPATH_RUNTIME_DATA_FILE));
    if (!objectExists(toKeyPath(path))) {
      return null;
    }

    AtomicInteger counter = _newBatch.apply("Reading runtime data", 1);
    try {
      InputStream objStream = getObjectStream(toKeyPath(path));
      return BatfishObjectMapper.mapper().readValue(objStream, SnapshotRuntimeData.class);
    } catch (IOException e) {
      _logger.warnf(
              "Unexpected exception caught while loading runtime data for snapshot %s: %s",
              snapshot, Throwables.getStackTraceAsString(e));
      LOGGER.warn(
              String.format(
                      "Unexpected exception caught while loading runtime data for snapshot %s", snapshot),
              e);
      return null;
    } finally {
      counter.incrementAndGet();
    }
  }

  /**
   * Stores the configuration information into the given testrig. Will replace any previously-stored
   * configurations.
   */
  @Override
  public void storeConfigurations(
          Map<String, Configuration> configurations,
          ConvertConfigurationAnswerElement convertAnswerElement,
          @Nullable Layer1Topology synthesizedLayer1Topology,
          NetworkId network,
          SnapshotId snapshot)
          throws IOException {

    // Save the convert configuration answer element.
    Path ccaePath = getConvertAnswerPath(network, snapshot);
    serializeObject(convertAnswerElement, ccaePath);

    // Save the synthesized layer1 topology
    if (synthesizedLayer1Topology != null) {
      storeSynthesizedLayer1Topology(synthesizedLayer1Topology, network, snapshot);
    }

    Path outputDir = getVendorIndependentConfigDir(network, snapshot);

    String batchName =
            String.format(
                    "Serializing %s vendor-independent configuration structures for snapshot %s",
                    configurations.size(), snapshot);

    storeConfigurations(outputDir, batchName, configurations);
  }

  @Override
  public void storeConversionContext(ConversionContext conversionContext, NetworkSnapshot snapshot)
          throws IOException {
    Path ccPath = getConversionContextPath(snapshot.getNetwork(), snapshot.getSnapshot());
    serializeObject(conversionContext, ccPath);
  }

  @VisibleForTesting
  @Nonnull
  Path getConversionContextPath(NetworkId network, SnapshotId snapshot) {
    return getSnapshotOutputDir(network, snapshot).resolve(RELPATH_CONVERSION_CONTEXT);
  }

  private @Nonnull Path getConvertAnswerPath(NetworkId network, SnapshotId snapshot) {
    return getSnapshotOutputDir(network, snapshot).resolve(RELPATH_CONVERT_ANSWER_PATH);
  }

  private @Nonnull Path getSynthesizedLayer1TopologyPath(NetworkId network, SnapshotId snapshot) {
    return getSnapshotOutputDir(network, snapshot).resolve(RELPATH_SYNTHESIZED_LAYER1_TOPOLOGY);
  }

  private void storeConfigurations(
          Path outputDir, String batchName, Map<String, Configuration> configurations)
          throws IOException {
    _logger.infof("\n*** %s***\n", batchName.toUpperCase());
    AtomicInteger progressCount = _newBatch.apply(batchName, configurations.size());

    // Delete any existing output, then recreate.
    deleteDirectory(outputDir);

    configurations.entrySet().parallelStream()
            .forEach(
                    e -> {
                      Path currentOutputPath = outputDir.resolve(e.getKey());
                      serializeObject(e.getValue(), currentOutputPath);
                      progressCount.incrementAndGet();
                    });
  }

  @Override
  public void storeAnswer(
          NetworkId network, SnapshotId snapshot, String answerStr, AnswerId answerId)
          throws IOException {
    Path answerPath = getAnswerPath(network, snapshot, answerId);
    writeStringToFile(answerPath, answerStr, UTF_8);
  }

  @Override
  public void storeAnswerMetadata(
          NetworkId networkId, SnapshotId snapshotId, AnswerMetadata answerMetadata, AnswerId answerId)
          throws IOException {
    Path answerMetadataPath = getAnswerMetadataPath(networkId, snapshotId, answerId);
    writeJsonFile(answerMetadataPath, answerMetadata);
  }

  /**
   * Returns a single object of the given class deserialized from the given file. Uses the {@link
   * S3BucketStorage} default file encoding including serialization format and compression.
   */
  @SuppressWarnings("PMD.CloseResource") // PMD does not understand Closer
  // TODO: this should be private still, only making it protected so it compiles while i debug
  private <S extends Serializable> S deserializeObject(Path inputFile, Class<S> outputClass)
          throws BatfishException {
    Path sanitizedInputFile = validatePath(inputFile);
    try (Closer closer = Closer.create()) {
      ResponseInputStream<GetObjectResponse> fis = closer.register(getObjectStream(toKeyPath(inputFile)));
      PushbackInputStream pbstream = new PushbackInputStream(fis, DEFAULT_HEADER_LENGTH_BYTES);
      PluginConsumer.Format f = detectFormat(pbstream);
      ObjectInputStream ois;
      if (f == PluginConsumer.Format.GZIP) {
        GZIPInputStream gis =
                closer.register(new GZIPInputStream(pbstream, 8192 /* enlarge buffer */));
        ois = new ObjectInputStream(gis);
      } else if (f == PluginConsumer.Format.LZ4) {
        LZ4FrameInputStream lis = closer.register(new LZ4FrameInputStream(pbstream));
        ois = new ObjectInputStream(lis);
      } else if (f == PluginConsumer.Format.JAVA_SERIALIZED) {
        ois = new ObjectInputStream(pbstream);
      } else {
        throw new BatfishException(
                String.format("Could not detect format of the file %s", sanitizedInputFile));
      }
      closer.register(ois);
      return outputClass.cast(ois.readObject());
    } catch (Exception e) {
      throw new BatfishException(
              String.format(
                      "Failed to deserialize object of type %s from file %s",
                      outputClass.getCanonicalName(), sanitizedInputFile),
              e);
    }
  }

  private <S extends Serializable> SortedMap<String, S> deserializeObjects(
          Map<Path, String> namesByPath, Class<S> outputClass) {
    String outputClassName = outputClass.getName();
    AtomicInteger completed =
            _newBatch.apply(
                    String.format("Deserializing objects of type '%s' from files", outputClassName),
                    namesByPath.size());
    return new TreeMap<>(
            namesByPath.entrySet().parallelStream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getValue,
                                    entry -> {
                                      Path inputPath = entry.getKey();
                                      String name = entry.getValue();
                                      _logger.debugf(
                                              "Reading %s '%s' from '%s'\n", outputClassName, name, inputPath);
                                      S output = deserializeObject(inputPath, outputClass);
                                      completed.incrementAndGet();
                                      return output;
                                    })));
  }

  /**
   * Writes a single object of the given class to the given file. Uses the {@link S3BucketStorage}
   * default file encoding including serialization format and compression.
   */
  @VisibleForTesting
  void serializeObject(Serializable object, Path outputFile) {
    String objKey = toKeyPath(validatePath(outputFile));
    try {
      Path tmpFile = Files.createTempFile(null, null);
      try {
        try (OutputStream out = Files.newOutputStream(tmpFile);
             LZ4FrameOutputStream gos = new LZ4FrameOutputStream(out);
             ObjectOutputStream oos = new ObjectOutputStream(gos)) {
          oos.writeObject(object);
        } catch (Throwable e) {
          throw new BatfishException(
                  "Failed to serialize object to output file: " + objKey, e);
        }
        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(_s3cfg.getBucketName())
                .key(objKey)
                .build();
        _s3Client.putObject(putRequest, tmpFile);
      } finally {
        Files.deleteIfExists(tmpFile);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private <S extends Serializable> void serializeObjects(Map<Path, S> objectsByPath) {
    if (objectsByPath.isEmpty()) {
      return;
    }
    int size = objectsByPath.size();
    String className = objectsByPath.values().iterator().next().getClass().getName();
    AtomicInteger serializeCompleted =
            _newBatch.apply(String.format("Serializing '%s' instances to disk", className), size);
    objectsByPath.entrySet().parallelStream()
            .forEach(
                    entry -> {
                      Path outputPath = entry.getKey();
                      S object = entry.getValue();
                      serializeObject(object, outputPath);
                      serializeCompleted.incrementAndGet();
                    });
  }

  private boolean cachedConfigsAreCompatible(NetworkId network, SnapshotId snapshot) {
    try {
      ConvertConfigurationAnswerElement ccae =
              loadConvertConfigurationAnswerElement(network, snapshot);
      return ccae != null;
    } catch (BatfishException e) {
      _logger.warnf(
              "Unexpected exception caught while deserializing configs for snapshot %s: %s",
              snapshot, Throwables.getStackTraceAsString(e));
      LOGGER.warn(
              String.format(
                      "Unexpected exception caught while deserializing configs for snapshot %s", snapshot),
              e);
      return false;
    }
  }

  @Override
  public @Nonnull String loadQuestion(
          NetworkId network, QuestionId question) throws IOException {
    return readFileToString(getQuestionPath(network, question), UTF_8);
  }

  @Override
  public boolean checkQuestionExists(
          NetworkId network, QuestionId question) {
    return objectExists(toKeyPath(getQuestionPath(network, question)));
  }

  private @Nonnull Path getQuestionPath(
          NetworkId network, QuestionId question) {
    return getQuestionDir(network, question).resolve(BfConsts.RELPATH_QUESTION_FILE);
  }

  @Override
  public @Nonnull String loadAnswer(NetworkId networkId, SnapshotId snapshotId, AnswerId answerId)
          throws IOException {
    Path answerPath = getAnswerPath(networkId, snapshotId, answerId);
    if (objectExists(toKeyPath(answerPath))) {
      return readFileToString(answerPath, UTF_8);
    }
    // look for the answer in the legacy location
    Path oldAnswerPath = getOldAnswerPath(answerId);
    if (objectExists(toKeyPath(oldAnswerPath))) {
      return readFileToString(oldAnswerPath, UTF_8);
    }
    throw new FileNotFoundException(String.format("Could not find answer with ID: %s", answerId));
  }

  @Override
  public @Nonnull AnswerMetadata loadAnswerMetadata(
          NetworkId networkId, SnapshotId snapshotId, AnswerId answerId) throws IOException {
    Path answerMetadataPath = getAnswerMetadataPath(networkId, snapshotId, answerId);
    if (objectExists(toKeyPath(answerMetadataPath))) {
      InputStream objStream = getObjectStream(toKeyPath(answerMetadataPath));
      return BatfishObjectMapper.mapper()
              .readValue(objStream, new TypeReference<AnswerMetadata>() {});
    }
    // look for the answer metadata in the legacy location
    Path oldAnswerMetadataPath = getOldAnswerMetadataPath(answerId);
    if (objectExists(toKeyPath(oldAnswerMetadataPath))) {
      InputStream objStream = getObjectStream(toKeyPath(oldAnswerMetadataPath));
      return BatfishObjectMapper.mapper()
              .readValue(objStream, new TypeReference<AnswerMetadata>() {});
    }
    throw new FileNotFoundException(
            String.format("Could not find answer metadata for ID: %s", answerId));
  }

  @VisibleForTesting
  @Nonnull
  Path getAnswerPath(NetworkId networkId, SnapshotId snapshotId, AnswerId answerId) {
    return getAnswerDir(networkId, snapshotId, answerId).resolve(RELPATH_ANSWER_JSON);
  }

  @VisibleForTesting
  @Nonnull
  Path getOldAnswerPath(AnswerId answerId) {
    return getOldAnswerDir(answerId).resolve(RELPATH_ANSWER_JSON);
  }

  private @Nonnull Path getAnswerMetadataPath(
          NetworkId networkId, SnapshotId snapshotId, AnswerId answerId) {
    return getAnswerDir(networkId, snapshotId, answerId).resolve(RELPATH_ANSWER_METADATA);
  }

  @Nonnull
  Path getOldAnswerMetadataPath(AnswerId answerId) {
    return getOldAnswerDir(answerId).resolve(RELPATH_ANSWER_METADATA);
  }

  @Override
  public void storeQuestion(
          String questionStr, NetworkId network, QuestionId question)
          throws IOException {
    Path questionPath = getQuestionPath(network, question);
    writeStringToFile(questionPath, questionStr, UTF_8);
  }

  @Override
  public boolean checkNetworkExists(NetworkId network) {
    String objKey = toKeyPath(getNetworkDir(network)) + FOLDER_SUFFIX;
    return objectExists(objKey);
  }

  @Override
  public boolean hasAnswerMetadata(NetworkId networkId, SnapshotId snapshotId, AnswerId answerId) {
    return objectExists(toKeyPath(getAnswerMetadataPath(networkId, snapshotId, answerId)))
            || objectExists(toKeyPath(getOldAnswerMetadataPath(answerId)));
  }

  @Override
  public String loadQuestionClassId(
          NetworkId networkId, QuestionId questionId) throws IOException {
    return Question.parseQuestion(loadQuestion(networkId, questionId)).getName();
  }

  private @Nonnull Path getSnapshotMetadataPath(NetworkId networkId, SnapshotId snapshotId) {
    return getSnapshotOutputDir(networkId, snapshotId).resolve(RELPATH_METADATA_FILE);
  }

  @Override
  public void storeSnapshotMetadata(
          SnapshotMetadata snapshotMetadata, NetworkId networkId, SnapshotId snapshotId)
          throws IOException {
    writeJsonFile(getSnapshotMetadataPath(networkId, snapshotId), snapshotMetadata);
  }

  @Override
  public String loadSnapshotMetadata(NetworkId networkId, SnapshotId snapshotId)
          throws FileNotFoundException, IOException {
    return readFileToString(getSnapshotMetadataPath(networkId, snapshotId), UTF_8);
  }

  @Override
  public void storeNodeRoles(
          NetworkId networkId, NodeRolesData nodeRolesData, NodeRolesId nodeRolesId)
          throws IOException {
    writeJsonFile(getNodeRolesPath(networkId, nodeRolesId), nodeRolesData);
  }

  private @Nonnull Path getNodeRolesPath(NetworkId networkId, NodeRolesId nodeRolesId) {
    return getNodeRolesDir(networkId).resolve(String.format("%s%s", nodeRolesId.getId(), ".json"));
  }

  @VisibleForTesting
  @Nonnull
  Path getOldNodeRolesPath(NodeRolesId nodeRolesId) {
    return getOldNodeRolesDir().resolve(String.format("%s%s", nodeRolesId.getId(), ".json"));
  }

  @Override
  public String loadNodeRoles(NetworkId networkId, NodeRolesId nodeRolesId) throws IOException {
    Path nodeRolesPath = getNodeRolesPath(networkId, nodeRolesId);
    if (objectExists(toKeyPath(nodeRolesPath))) {
      return readFileToString(getNodeRolesPath(networkId, nodeRolesId), UTF_8);
    }
    // try at the legacy location
    return readFileToString(getOldNodeRolesPath(nodeRolesId), UTF_8);
  }

  @Override
  public boolean hasNodeRoles(NetworkId networkId, NodeRolesId nodeRolesId) {
    return objectExists(toKeyPath(getNodeRolesPath(networkId, nodeRolesId)))
            || objectExists(toKeyPath(getOldNodeRolesPath(nodeRolesId)));
  }

  @Override
  public void initNetwork(NetworkId networkId) {
    String networkPath = toKeyPath(getNetworkDir(networkId));
    try {
      createFolder(Path.of(networkPath));
    } catch (Exception e) {
      _logger.warnf("Unable to initialize network: %s", e.getMessage());
    }
  }

  @Override
  public void deleteAnswerMetadata(NetworkId networkId, SnapshotId snapshotId, AnswerId answerId)
          throws FileNotFoundException, IOException {
    deleteObjects(getAnswerMetadataPath(networkId, snapshotId, answerId));
  }

  /** {@code key} must be relative normalized path. */
  @VisibleForTesting
  static @Nonnull Path objectKeyToRelativePath(String key) {
    Path relativePathCandidate = Paths.get(FilenameUtils.separatorsToSystem(key));
    // ensure path is relative
    checkArgument(
            relativePathCandidate.getRoot() == null,
            "Key '%s' does not represent a relative path",
            key);
    // ensure path is normalized
    checkArgument(
            relativePathCandidate.equals(relativePathCandidate.normalize()),
            "Key '%s' does not represent a normalized path  (without '.', '..',  etc.)",
            key);
    return relativePathCandidate;
  }

  private @Nonnull Path getNetworkObjectPath(NetworkId networkId, String key) {
    String encodedKey = toBase64(key);
    return getNetworkObjectsDir(networkId).resolve(encodedKey);
  }

  @Override
  public @Nonnull InputStream loadNetworkObject(NetworkId networkId, String key)
          throws FileNotFoundException, IOException {
    Path objectPath = getNetworkObjectPath(networkId, key);
    if (!objectExists(toKeyPath(objectPath))) {
      throw new FileNotFoundException(String.format("Could not load: %s", objectPath));
    }
    return getObjectStream(toKeyPath(objectPath));
  }

  @Override
  @SuppressWarnings("PMD.UseTryWithResources") // syntax is awkward to close stream you don't open
  public void storeNetworkObject(InputStream inputStream, NetworkId networkId, String key)
          throws IOException {
    Path objectPath = getNetworkObjectPath(networkId, key);
    writeStreamToFile(inputStream, objectPath);
  }

  @VisibleForTesting
  @Nonnull
  Path getNetworkBlobPath(NetworkId networkId, String key) {
    String encodedKey = toBase64(key);
    return getNetworkBlobsDir(networkId).resolve(encodedKey);
  }

  @Override
  public @Nonnull InputStream loadNetworkBlob(NetworkId networkId, String key)
          throws FileNotFoundException, IOException {
    Path objectPath = getNetworkBlobPath(networkId, key);
    if (!objectExists(toKeyPath(objectPath))) {
      throw new FileNotFoundException(String.format("Could not load: %s", objectPath));
    }
    return getObjectStream(toKeyPath(objectPath));
  }

  @Override
  @SuppressWarnings("PMD.UseTryWithResources") // syntax is awkward to close stream you don't open
  public void storeNetworkBlob(InputStream inputStream, NetworkId networkId, String key)
          throws IOException {
    Path objectPath = getNetworkBlobPath(networkId, key);
    writeStreamToFile(inputStream, objectPath);
  }

  @Override
  public void deleteNetworkObject(NetworkId networkId, String key)
          throws FileNotFoundException, IOException {
    Path objectPath = getNetworkObjectPath(networkId, key);
    if (!objectExists(toKeyPath(objectPath))) {
      throw new FileNotFoundException(String.format("Could not delete: %s", objectPath));
    }
    deleteObjects(objectPath);
  }

  private @Nonnull Path getSnapshotObjectPath(
          NetworkId networkId, SnapshotId snapshotId, String key) {
    String encodedKey = toBase64(key);
    return getSnapshotObjectsDir(networkId, snapshotId).resolve(encodedKey);
  }

  public static @Nonnull String toBase64(String key) {
    return Base64.getUrlEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
  }

  public static @Nonnull String fromBase64(String key) {
    return new String(Base64.getUrlDecoder().decode(key), StandardCharsets.UTF_8);
  }

  @Override
  public @Nonnull InputStream loadSnapshotObject(
          NetworkId networkId, SnapshotId snapshotId, String key)
          throws FileNotFoundException, IOException {
    Path objectPath = getSnapshotObjectPath(networkId, snapshotId, key);
    if (!objectExists(toKeyPath(objectPath))) {
      throw new FileNotFoundException(String.format("Could not load: %s", objectPath));
    }
    return getObjectStream(toKeyPath(objectPath));
  }

  @Override
  @SuppressWarnings("PMD.UseTryWithResources") // syntax is awkward to close stream you don't open
  public void storeSnapshotObject(
          InputStream inputStream, NetworkId networkId, SnapshotId snapshotId, String key)
          throws IOException {
    Path objectPath = getSnapshotObjectPath(networkId, snapshotId, key);
    writeStreamToFile(inputStream, objectPath);
  }

  @Override
  public void deleteSnapshotObject(NetworkId networkId, SnapshotId snapshotId, String key)
          throws FileNotFoundException, IOException {
    Path objectPath = getSnapshotObjectPath(networkId, snapshotId, key);
    if (!objectExists(toKeyPath(objectPath))) {
      throw new FileNotFoundException(String.format("Could not delete: %s", objectPath));
    }
    deleteObjects(objectPath);
  }

  @MustBeClosed
  @Override
  public @Nonnull InputStream loadSnapshotInputObject(
          NetworkId networkId, SnapshotId snapshotId, String key)
          throws FileNotFoundException, IOException {
    String objKey = toKeyPath(getSnapshotInputObjectPath(networkId, snapshotId, key));
    try {
      return getObjectStream(objKey);
    } catch (NoSuchKeyException e) {
      // Calling functions will be checking for FileNotFoundException and can handle that appropriately,
      // throwing a NoSuchKeyException results in failure to process snapshot
      throw new FileNotFoundException(String.format("Could not load: %s", objKey));
    }
  }

  @Override
  public boolean hasSnapshotInputObject(String key, NetworkSnapshot snapshot) throws IOException {
    Path snapshotPath =getSnapshotInputObjectPath(snapshot.getNetwork(), snapshot.getSnapshot(), key);
    return objectExists(toKeyPath(snapshotPath));
  }

  @Override
  public @Nonnull List<StoredObjectMetadata> getSnapshotInputObjectsMetadata(
          NetworkId networkId, SnapshotId snapshotId) throws IOException {
    Path objectPath = getSnapshotInputObjectsDir(networkId, snapshotId);
    if (!objectExists(toKeyPath(objectPath))) {
      throw new FileNotFoundException(String.format("Could not load: %s", objectPath));
    }

    try {
      return listFolder(objectPath)
              .map(obj -> fromKeyPath(relativizeKeyPath(objectPath, obj.key())))
              .map(path -> new StoredObjectMetadata(path.toString(), getObjectSize(path)))
              .collect(ImmutableList.toImmutableList());
    } catch (BatfishException e) {
      throw new IOException(e);
    }
  }

  @Override
  public @Nonnull List<StoredObjectMetadata> getSnapshotExtendedObjectsMetadata(
          NetworkId networkId, SnapshotId snapshotId) throws IOException {
    Path objectPath = getSnapshotObjectsDir(networkId, snapshotId);
    if (!objectExists(toKeyPath(objectPath))) {
      throw new FileNotFoundException(String.format("Could not load: %s", objectPath));
    }

    try {
      return listFolder(objectPath)
              .map(obj -> fromKeyPath(obj.key()))
              .map(
                      obj ->
                              new StoredObjectMetadata(
                                      fromBase64(obj.getFileName().toString()), getObjectSize(obj)))
              .collect(ImmutableList.toImmutableList());
    } catch (BatfishException e) {
      throw new IOException(e);
    }
  }

  private long getObjectSize(Path objectPath) {
    try {
      HeadObjectRequest headRequest = HeadObjectRequest.builder()
              .bucket(_s3cfg.getBucketName())
              .key(toKeyPath(objectPath))
              .build();
      HeadObjectResponse headResponse = _s3Client.headObject(headRequest);
      return headResponse.contentLength();
    } catch (SdkException e) {
      throw new BatfishException(
              String.format("Could not get size of object at path: %s", objectPath), e);
    }
  }

  @VisibleForTesting
  Path getSnapshotInputObjectPath(NetworkId networkId, SnapshotId snapshotId, String key) {
    Path relativePath = objectKeyToRelativePath(key);
    return getSnapshotInputObjectsDir(networkId, snapshotId).resolve(relativePath);
  }

  @Override
  public @Nonnull String loadPojoTopology(NetworkId networkId, SnapshotId snapshotId)
          throws IOException {
    return readFileToString(getPojoTopologyPath(networkId, snapshotId), UTF_8);
  }

  private @Nonnull Path getBgpTopologyPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_BGP_TOPOLOGY);
  }

  private @Nonnull Path getEigrpTopologyPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_EIGRP_TOPOLOGY);
  }

  private @Nonnull Path getLayer3TopologyPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_LAYER3_TOPOLOGY);
  }

  private @Nonnull Path getL3AdjacenciesPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_L3_ADJACENCIES);
  }

  private @Nonnull Path getOspfTopologyPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_OSPF_TOPOLOGY);
  }

  private @Nonnull Path getVxlanTopologyPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_VXLAN_TOPOLOGY);
  }

  private @Nonnull Path getPojoTopologyPath(NetworkId networkId, SnapshotId snapshotId) {
    return getSnapshotOutputDir(networkId, snapshotId).resolve(RELPATH_TESTRIG_POJO_TOPOLOGY_PATH);
  }

  @Nonnull
  private Path getWorkLogPath(NetworkId network, SnapshotId snapshot, String workId) {
    return getSnapshotOutputDir(network, snapshot).resolve(toBase64(workId + SUFFIX_LOG_FILE));
  }

  @Nonnull
  private Path getWorkJsonPath(NetworkId network, SnapshotId snapshot, String workId) {
    return getSnapshotOutputDir(network, snapshot)
            .resolve(toBase64(workId + SUFFIX_ANSWER_JSON_FILE));
  }

  @Override
  public @Nonnull String loadInitialTopology(NetworkId networkId, SnapshotId snapshotId)
          throws IOException {
    Path path = getEnvTopologyPath(networkId, snapshotId);
    return readFileToString(path, UTF_8);
  }

  private @Nonnull Path getEnvTopologyPath(NetworkId networkId, SnapshotId snapshotId) {
    return getSnapshotOutputDir(networkId, snapshotId).resolve(RELPATH_ENV_TOPOLOGY_FILE);
  }

  @Override
  public void storeInitialTopology(Topology topology, NetworkId networkId, SnapshotId snapshotId)
          throws IOException {
    Path path = getEnvTopologyPath(networkId, snapshotId);
    writeJsonFile(path, topology);
  }

  @Override
  public void storePojoTopology(
          org.batfish.datamodel.pojo.Topology topology, NetworkId networkId, SnapshotId snapshotId)
          throws IOException {
    Path path = getPojoTopologyPath(networkId, snapshotId);
    writeStringToFile(path, BatfishObjectMapper.writeString(topology), UTF_8);
  }

  @Override
  public void storeWorkLog(String logOutput, NetworkId network, SnapshotId snapshot, String workId)
          throws IOException {
    writeStringToFile(getWorkLogPath(network, snapshot, workId), logOutput, UTF_8);
  }

  @Override
  public void storeWorkJson(
          String jsonOutput, NetworkId network, SnapshotId snapshot, String workId) throws IOException {
    writeStringToFile(getWorkJsonPath(network, snapshot, workId), jsonOutput, UTF_8);
  }

  @Override
  public CompletionMetadata loadCompletionMetadata(NetworkId networkId, SnapshotId snapshotId)
          throws IOException {
    Path completionMetadataPath = getSnapshotCompletionMetadataPath(networkId, snapshotId);
    if (!objectExists(toKeyPath(completionMetadataPath))) {
      return CompletionMetadata.EMPTY;
    }
    InputStream objStream = getObjectStream(toKeyPath(completionMetadataPath));
    return BatfishObjectMapper.mapper()
            .readValue(objStream, CompletionMetadata.class);
  }

  @Override
  public void storeCompletionMetadata(
          CompletionMetadata completionMetadata, NetworkId networkId, SnapshotId snapshotId)
          throws IOException {
    Path completionMetadataPath = getSnapshotCompletionMetadataPath(networkId, snapshotId);
    writeJsonFile(completionMetadataPath, completionMetadata);
  }

  private @Nonnull Path getSnapshotCompletionMetadataPath(
          NetworkId networkId, SnapshotId snapshotId) {
    return getSnapshotOutputDir(networkId, snapshotId).resolve(RELPATH_COMPLETION_METADATA_FILE);
  }

  @VisibleForTesting
  @Nonnull
  Path validatePath(Path path) {
    try {
      Path sanitizedPath = path.toFile().getCanonicalFile().toPath();
      checkArgument(
              sanitizedPath.toString().startsWith(_baseDir.toString()),
              "Path %s outside of base dir %s",
              path,
              _baseDir);
      return sanitizedPath;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Read the contents of a file into a string, using the provided input charset.
   *
   * @throws FileNotFoundException if the file does not exist or is otherwise inaccessible
   * @throws IOException if there is any other error
   */
  @Nonnull String readFileToString(Path file, Charset charset) throws SdkClientException, IOException {
    String filePath = toKeyPath(file);
    try {
      String objAsString = new String(getObjectStream(filePath).readAllBytes(), UTF_8);
      return objAsString;
    } catch (NoSuchKeyException e) {
      throw new FileNotFoundException(e.getMessage());
    }
  }

  @VisibleForTesting
  void writeStringToFile(Path file, CharSequence data, Charset charset) throws SdkClientException {
    PutObjectRequest request = PutObjectRequest.builder()
            .bucket(_s3cfg.getBucketName())
            .key(toKeyPath(file))
            .build();
    _s3Client.putObject(request, RequestBody.fromString(data.toString()));
  }

  void writeJsonFile(Path file, @Nullable Object json) throws IOException, SdkClientException {
    // writeValue expects either a file or an outputstream. S3 SDK does not support outputstream
    // so we'll use a scratch space temp file and upload that
    Path tmpFile = Files.createTempFile(null, null);
    String folder = toKeyPath(file.getParent()) + FOLDER_SUFFIX;
    try {
      BatfishObjectMapper.writer().writeValue(tmpFile.toFile(), json);
      String key = folder + file.getFileName().toString();
      PutObjectRequest putRequest = PutObjectRequest.builder()
              .bucket(_s3cfg.getBucketName())
              .key(key)
              .build();
      _s3Client.putObject(putRequest, tmpFile);
    } finally {
      Files.deleteIfExists(tmpFile);
    }
  }

  void writeStreamToFile(InputStream inputStream, Path outputFile) throws SdkClientException, IOException {
    String outputKeyPath = toKeyPath(outputFile);
    PutObjectRequest request = PutObjectRequest.builder()
            .bucket(_s3cfg.getBucketName())
            .key(outputKeyPath)
            .build();

    // We wont know the size of the zipfile being uploaded due to multipart-form-data uploads passing
    // in an InputStream. We could convert this to a byte buffer but that would load the entire contents
    // of the zipfile into memory. Instead we'll stash the file to scratch temp space and then upload
    // that file to our bucket instead.
    Path tmpFile = Files.createTempFile(null, null);
    try {
      FileUtils.copyInputStreamToFile(inputStream, tmpFile.toFile());
      _s3Client.putObject(request, tmpFile);
    } finally {
      Files.deleteIfExists(tmpFile);
    }
  }

  @Override
  public @Nonnull BgpTopology loadBgpTopology(NetworkSnapshot networkSnapshot) throws IOException {
    Path bgpTopoPath = getBgpTopologyPath(networkSnapshot);
    InputStream objStream = getObjectStream(toKeyPath(bgpTopoPath));
    return BatfishObjectMapper.mapper()
            .readValue(objStream, BgpTopology.class);
  }

  @Override
  public @Nonnull EigrpTopology loadEigrpTopology(NetworkSnapshot networkSnapshot)
          throws IOException {
    Path eigrpTopoPath = getEigrpTopologyPath(networkSnapshot);
    InputStream objStream = getObjectStream(toKeyPath(eigrpTopoPath));
    return BatfishObjectMapper.mapper()
            .readValue(objStream, EigrpTopology.class);
  }

  @Nonnull
  @Override
  public Optional<Layer1Topology> loadSynthesizedLayer1Topology(NetworkSnapshot snapshot)
          throws IOException {
    Path sl1tPath = getSynthesizedLayer1TopologyPath(snapshot.getNetwork(), snapshot.getSnapshot());
    // this is here for backward compatibility when we load up an existing container
    if (!objectExists(toKeyPath(sl1tPath))) {
      return Optional.empty();
    }
    InputStream objStream = getObjectStream(toKeyPath(sl1tPath));
    return Optional.ofNullable(
            BatfishObjectMapper.mapper().readValue(objStream, Layer1Topology.class));
  }

  @Override
  public @Nonnull Topology loadLayer3Topology(NetworkSnapshot networkSnapshot) throws IOException {
    InputStream objStream = getObjectStream(toKeyPath(getLayer3TopologyPath(networkSnapshot)));
    return BatfishObjectMapper.mapper()
            .readValue(objStream, Topology.class);
  }

  @Nonnull
  @Override
  public L3Adjacencies loadL3Adjacencies(NetworkSnapshot networkSnapshot) throws IOException {
    return deserializeObject(getL3AdjacenciesPath(networkSnapshot), L3Adjacencies.class);
  }

  @Override
  public @Nonnull OspfTopology loadOspfTopology(NetworkSnapshot networkSnapshot)
          throws IOException {
    Path ospfTopoPath = getOspfTopologyPath(networkSnapshot);
    InputStream objStream = getObjectStream(toKeyPath(ospfTopoPath));
    return BatfishObjectMapper.mapper()
            .readValue(objStream, OspfTopology.class);
  }

  @Override
  public @Nonnull VxlanTopology loadVxlanTopology(NetworkSnapshot networkSnapshot)
          throws IOException {
    InputStream objStream = getObjectStream(toKeyPath(getVxlanTopologyPath(networkSnapshot)));
    return BatfishObjectMapper.mapper()
            .readValue(objStream, VxlanTopology.class);
  }

  @Override
  public void storeBgpTopology(BgpTopology bgpTopology, NetworkSnapshot networkSnapshot)
          throws IOException {
    Path path = getBgpTopologyPath(networkSnapshot);
    writeJsonFile(path, bgpTopology);
  }

  @Override
  public void storeEigrpTopology(EigrpTopology eigrpTopology, NetworkSnapshot networkSnapshot)
          throws IOException {
    Path path = getEigrpTopologyPath(networkSnapshot);
    writeJsonFile(path, eigrpTopology);
  }

  @Override
  public void storeL3Adjacencies(L3Adjacencies l3Adjacencies, NetworkSnapshot networkSnapshot)
          throws IOException {
    Path path = getL3AdjacenciesPath(networkSnapshot);
    serializeObject(l3Adjacencies, path);
  }

  @Override
  public void storeLayer3Topology(Topology layer3Topology, NetworkSnapshot networkSnapshot)
          throws IOException {
    Path path = getLayer3TopologyPath(networkSnapshot);
    writeJsonFile(path, layer3Topology);
  }

  @Override
  public void storeOspfTopology(OspfTopology ospfTopology, NetworkSnapshot networkSnapshot)
          throws IOException {
    Path path = getOspfTopologyPath(networkSnapshot);
    writeJsonFile(path, ospfTopology);
  }

  @Override
  public void storeVxlanTopology(VxlanTopology vxlanTopology, NetworkSnapshot networkSnapshot)
          throws IOException {
    Path path = getVxlanTopologyPath(networkSnapshot);
    writeJsonFile(path, vxlanTopology);
  }

  @VisibleForTesting
  void storeSynthesizedLayer1Topology(
          Layer1Topology synthesizedLayer1Topology, NetworkId network, SnapshotId snapshot)
          throws IOException {
    Path sl1tPath = getSynthesizedLayer1TopologyPath(network, snapshot);
    writeJsonFile(sl1tPath, synthesizedLayer1Topology);
  }

  @Override
  public @Nonnull Optional<String> readId(Class<? extends Id> idType, String name, Id... ancestors)
          throws IOException {
    try {
      return Optional.of(readFileToString(getIdFile(idType, name, ancestors), UTF_8));
    } catch (FileNotFoundException e) {
      return Optional.empty();
    }
  }

  @Override
  public void writeId(Id id, String name, Id... ancestors) throws IOException {
    Path file = getIdFile(id.getClass(), name, ancestors);
    writeStringToFile(file, id.getId(), UTF_8);
  }

  @Override
  public boolean deleteNameIdMapping(Class<? extends Id> type, String name, Id... ancestors)
          throws IOException {
    return deleteObjects(getIdFile(type, name, ancestors));
  }

  @Override
  public boolean hasId(Class<? extends Id> type, String name, Id... ancestors) {
    return objectExists(toKeyPath(getIdFile(type, name, ancestors)));
  }

  @Override
  public @Nonnull Set<String> listResolvableNames(Class<? extends Id> type, Id... ancestors)
          throws IOException {
    Path idsDir = getIdsDir(type, ancestors);
    if (!isDirectory(idsDir)) {
      return ImmutableSet.of();
    }
    try (Stream<S3Object> objs = listFolder(idsDir)) {
      return objs
              .map(obj -> obj.key())
              .map(obj -> StringUtils.substringAfterLast(obj, "/"))
              .filter(
                      obj -> {
                        try {
                          return fromBase64(obj).endsWith(ID_EXTENSION);
                        } catch (IllegalArgumentException e) {
                          return false;
                        }
                      })
              .map(FileBasedStorage::fromBase64)
              .map(
                      nameWithExtension ->
                              nameWithExtension.substring(
                                      0, nameWithExtension.length() - ID_EXTENSION.length()))
              .collect(ImmutableSet.toImmutableSet());
    }
  }

  /** Returns ids (as strings) that can ever be returned via a name-to-id mapping */
  private @Nonnull Set<String> listResolvedIds(Class<? extends Id> type, Id... ancestors)
          throws IOException {
    return listResolvableNames(type, ancestors).stream()
            .map(
                    name -> {
                      try {
                        return readId(type, name, ancestors);
                      } catch (IOException e) {
                        _logger.errorf("Could not read id for '%s' (type '%s')", name, type);
                        return Optional.<String>empty();
                      }
                    })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(ImmutableSet.toImmutableSet());
  }

  @Nonnull
  @Override
  public Optional<ReferenceLibrary> loadReferenceLibrary(NetworkId network) throws IOException {
    Path path = getReferenceLibraryPath(network);
    if (!objectExists(toKeyPath(path))) {
      return Optional.empty();
    }
    InputStream objStream = getObjectStream(toKeyPath(getReferenceLibraryPath(network)));
    return Optional.of(
            BatfishObjectMapper.mapper()
                    .readValue(objStream, ReferenceLibrary.class));
  }

  @Override
  public void storeReferenceLibrary(ReferenceLibrary referenceLibrary, NetworkId network)
          throws IOException {
    writeJsonFile(getReferenceLibraryPath(network), referenceLibrary);
  }

  @Override
  public void storeUploadSnapshotZip(InputStream inputStream, String key, NetworkId network)
          throws IOException {
    writeStreamToFile(inputStream, getUploadSnapshotZipPath(key, network));
  }

  private @Nonnull Path getUploadSnapshotZipPath(String key, NetworkId network) {
    return getOriginalDir(key, network).resolve(RELPATH_SNAPSHOT_ZIP_FILE);
  }

  @Override
  public void storeForkSnapshotRequest(String forkSnapshotRequest, String key, NetworkId network)
          throws IOException {
    writeStringToFile(getForkSnapshotRequestPath(key, network), forkSnapshotRequest, UTF_8);
  }

  private @Nonnull Path getForkSnapshotRequestPath(String key, NetworkId network) {
    return getOriginalDir(key, network).resolve(RELPATH_FORK_REQUEST_FILE);
  }

  private static final int STREAMED_FILE_BUFFER_SIZE = 1024;

  @MustBeClosed
  @Nonnull
  @Override
  public InputStream loadUploadSnapshotZip(String key, NetworkId network) throws IOException {
    String filePath = toKeyPath(getUploadSnapshotZipPath(key, network));
    return getObjectStream(filePath);
  }

  @Override
  public void storeSnapshotInputObject(
          InputStream inputStream, String key, NetworkSnapshot snapshot) throws IOException {
    writeStreamToFile(
            inputStream,
            getSnapshotInputObjectPath(snapshot.getNetwork(), snapshot.getSnapshot(), key));
  }

  @MustBeClosed
  @Nonnull
  @Override
  public Stream<String> listSnapshotInputObjectKeys(NetworkSnapshot snapshot) throws IOException {
    Path inputObjectsPath =
            getSnapshotInputObjectsDir(snapshot.getNetwork(), snapshot.getSnapshot());
    if (!isDirectory(inputObjectsPath)) {
      if (!isDirectory(inputObjectsPath.getParent())) {
        throw new FileNotFoundException(String.format("Missing snapshot dir for %s", snapshot));
      }
      // snapshot contained no input objects
      return Stream.empty();
    }

    String prefixToWalk = toKeyPath(inputObjectsPath);
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
            .bucket(_s3cfg.getBucketName())
            .prefix(prefixToWalk)
            .build();
    ListObjectsV2Iterable paginatedResponse = _s3Client.listObjectsV2Paginator(listRequest);
    return paginatedResponse.contents()
            .stream()
            .map(obj -> relativizeKeyPath(inputObjectsPath, obj.key()));
  }

  @Nonnull
  @Override
  public DataPlane loadDataPlane(NetworkSnapshot snapshot) throws IOException {
    return deserializeObject(getDataPlanePath(snapshot), DataPlane.class);
  }

  @Override
  public void storeDataPlane(DataPlane dataPlane, NetworkSnapshot snapshot) throws IOException {
    serializeObject(dataPlane, getDataPlanePath(snapshot));
  }

  @Override
  public boolean hasDataPlane(NetworkSnapshot snapshot) throws IOException {
    return objectExists(toKeyPath(getDataPlanePath(snapshot)));
  }

  @MustBeClosed
  @Nonnull
  @Override
  public Stream<String> listInputEnvironmentBgpTableKeys(NetworkSnapshot snapshot)
          throws IOException {
    return listSnapshotInputObjectKeys(snapshot)
            .filter(key -> key.startsWith(BfConsts.RELPATH_ENVIRONMENT_BGP_TABLES));
  }

  @Nonnull
  @Override
  public ParseEnvironmentBgpTablesAnswerElement loadParseEnvironmentBgpTablesAnswerElement(
          NetworkSnapshot snapshot) throws IOException {
    return deserializeObject(
            getParseEnvironmentBgpTablesAnswerElementPath(snapshot),
            ParseEnvironmentBgpTablesAnswerElement.class);
  }

  @Override
  public void storeParseEnvironmentBgpTablesAnswerElement(
          ParseEnvironmentBgpTablesAnswerElement parseEnvironmentBgpTablesAnswerElement,
          NetworkSnapshot snapshot)
          throws IOException {
    serializeObject(
            parseEnvironmentBgpTablesAnswerElement,
            getParseEnvironmentBgpTablesAnswerElementPath(snapshot));
  }

  @Override
  public boolean hasParseEnvironmentBgpTablesAnswerElement(NetworkSnapshot snapshot)
          throws IOException {
    return objectExists(toKeyPath(getParseEnvironmentBgpTablesAnswerElementPath(snapshot)));
  }

  @Override
  public void deleteParseEnvironmentBgpTablesAnswerElement(NetworkSnapshot snapshot)
          throws IOException {
    deleteObjects(getParseEnvironmentBgpTablesAnswerElementPath(snapshot));
  }

  private @Nonnull Path getParseEnvironmentBgpTablesAnswerElementPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_ENVIRONMENT_BGP_TABLES_ANSWER);
  }

  @Nonnull
  @Override
  public Map<String, BgpAdvertisementsByVrf> loadEnvironmentBgpTables(NetworkSnapshot snapshot)
          throws IOException {
    _logger.info("\n*** DESERIALIZING ENVIRONMENT BGP TABLES ***\n");
    _logger.resetTimer();
    Map<Path, String> namesByPath = new HashMap<>();
    Path dir = getEnvironmentBgpTablesPath(snapshot);
    if (!isDirectory(dir)) {
      return ImmutableSortedMap.of();
    }
    try (Stream<S3Object> serializedBgpTables = listFolder(dir)) {
      serializedBgpTables.forEach(serializedBgpTable -> {
        String name = StringUtils.substringAfterLast(serializedBgpTable.key(), "/");
        namesByPath.put(fromKeyPath(serializedBgpTable.key()), name);
      });
    } catch (SdkException e) {
      throw new IOException("Error reading serialized BGP tables", e);
    }

    SortedMap<String, BgpAdvertisementsByVrf> bgpTables =
            deserializeObjects(namesByPath, BgpAdvertisementsByVrf.class);
    _logger.printElapsedTime();
    return bgpTables;
  }

  @Override
  public void storeEnvironmentBgpTables(
          Map<String, BgpAdvertisementsByVrf> environmentBgpTables, NetworkSnapshot snapshot)
          throws IOException {
    _logger.info("\n*** SERIALIZING ENVIRONMENT BGP TABLES ***\n");
    _logger.resetTimer();
    SortedMap<Path, BgpAdvertisementsByVrf> output = new TreeMap<>();
    Path outputPath = getEnvironmentBgpTablesPath(snapshot);
    environmentBgpTables.forEach(
            (name, rt) -> {
              Path currentOutputPath = outputPath.resolve(name);
              output.put(currentOutputPath, rt);
            });
    serializeObjects(output);
    _logger.printElapsedTime();
  }

  @Override
  public void deleteEnvironmentBgpTables(NetworkSnapshot snapshot) throws IOException {
    deleteObjects(getEnvironmentBgpTablesPath(snapshot));
  }

  @Nonnull
  @Override
  public Optional<String> loadExternalBgpAnnouncementsFile(NetworkSnapshot snapshot)
          throws IOException {
    Path path =
            getSnapshotInputObjectPath(
                    snapshot.getNetwork(),
                    snapshot.getSnapshot(),
                    BfConsts.RELPATH_EXTERNAL_BGP_ANNOUNCEMENTS);
    if (!objectExists(toKeyPath(path))) {
      return Optional.empty();
    }
    return Optional.of(readFileToString(path, UTF_8));
  }

  private @Nonnull Path getEnvironmentBgpTablesPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_SERIALIZED_ENVIRONMENT_BGP_TABLES);
  }

  private @Nonnull Path getDataPlanePath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_DATA_PLANE);
  }

  private @Nonnull Path getReferenceLibraryPath(NetworkId network) {
    return getNetworkDir(network).resolve(BfConsts.RELPATH_REFERENCE_LIBRARY_PATH);
  }

  private static String toIdDirName(Class<? extends Id> type) {
    return toBase64(type.getCanonicalName());
  }

  private @Nonnull Path getIdsDir(Class<? extends Id> type, Id... ancestors) {
    Path file = getStorageBase().resolve("ids");
    for (Id id : ancestors) {
      file = file.resolve(toIdDirName(id.getClass())).resolve(id.getId());
    }
    return file.resolve(toIdDirName(type));
  }

  private @Nonnull Path getIdFile(Class<? extends Id> type, String name, Id... ancestors) {
    return getIdsDir(type, ancestors).resolve(toBase64(name + ID_EXTENSION));
  }

  private static final String RELPATH_BLOBS = "blobs";
  private static final String RELPATH_EXTENDED = "extended";
  private static final String RELPATH_NODE_ROLES_DIR = "node_roles";

  private @Nonnull Path getAdHocQuestionDir(NetworkId network, QuestionId question) {
    return getAdHocQuestionsDir(network).resolve(question.getId());
  }

  private @Nonnull Path getAdHocQuestionsDir(NetworkId networkId) {
    return getNetworkDir(networkId).resolve(RELPATH_QUESTIONS_DIR);
  }

  @VisibleForTesting
  @Nonnull
  Path getAnswersDir(NetworkId networkId, SnapshotId snapshotId) {
    return getSnapshotOutputDir(networkId, snapshotId).resolve(RELPATH_ANSWERS_DIR);
  }

  private @Nonnull Path getOldAnswersDir() {
    return _baseDir.resolve(RELPATH_ANSWERS_DIR);
  }

  @VisibleForTesting
  @Nonnull
  Path getAnswerDir(NetworkId networkId, SnapshotId snapshotId, AnswerId answerId) {
    return getAnswersDir(networkId, snapshotId).resolve(answerId.getId());
  }

  @Nonnull
  private Path getOldAnswerDir(AnswerId answerId) {
    return getOldAnswersDir().resolve(answerId.getId());
  }

  private @Nonnull Path getNetworksDir() {
    return _baseDir.resolve("networks");
  }

  @VisibleForTesting
  @Nonnull
  Path getNetworkDir(NetworkId network) {
    return getNetworksDir().resolve(network.getId());
  }

  /** Directory where original initialization or fork requests are stored */
  private @Nonnull Path getOriginalsDir(NetworkId network) {
    return getNetworkDir(network).resolve(RELPATH_ORIGINAL_DIR);
  }

  /** Directory where original initialization or fork request is stored */
  @Nonnull
  Path getOriginalDir(String key, NetworkId network) {
    return getOriginalsDir(network).resolve(toBase64(key));
  }

  private Path getNodeRolesDir(NetworkId networkId) {
    return getNetworkDir(networkId).resolve(RELPATH_NODE_ROLES_DIR);
  }

  private Path getOldNodeRolesDir() {
    return _baseDir.resolve(RELPATH_NODE_ROLES_DIR);
  }

  private @Nonnull Path getQuestionDir(NetworkId network, QuestionId question) {
    return getAdHocQuestionDir(network, question);
  }

  @VisibleForTesting
  @Nonnull
  Path getSnapshotsDir(NetworkId network) {
    return getNetworkDir(network).resolve(RELPATH_SNAPSHOTS_DIR);
  }

  @VisibleForTesting
  @Nonnull
  Path getSnapshotDir(NetworkId network, SnapshotId snapshot) {
    return getSnapshotsDir(network).resolve(snapshot.getId());
  }

  @VisibleForTesting
  @Nonnull
  Path getStorageBase() {
    return _baseDir;
  }

  private @Nonnull Path getVendorIndependentConfigDir(NetworkId network, SnapshotId snapshot) {
    return getSnapshotOutputDir(network, snapshot).resolve(RELPATH_VENDOR_INDEPENDENT_CONFIG_DIR);
  }

  @VisibleForTesting
  Path getNetworkBlobsDir(NetworkId networkId) {
    return getNetworkDir(networkId).resolve(RELPATH_BLOBS);
  }

  private Path getNetworkObjectsDir(NetworkId networkId) {
    return getNetworkDir(networkId).resolve(RELPATH_EXTENDED);
  }

  private Path getSnapshotObjectsDir(NetworkId networkId, SnapshotId snapshotId) {
    return getSnapshotDir(networkId, snapshotId).resolve(RELPATH_EXTENDED);
  }

  @VisibleForTesting
  @Nonnull
  Path getSnapshotInputObjectsDir(NetworkId networkId, SnapshotId snapshotId) {
    return getSnapshotDir(networkId, snapshotId).resolve(RELPATH_INPUT);
  }

  @VisibleForTesting
  Path getSnapshotOutputDir(NetworkId networkId, SnapshotId snapshotId) {
    return getSnapshotDir(networkId, snapshotId).resolve(RELPATH_OUTPUT);
  }

  @Nonnull
  @Override
  public ParseVendorConfigurationAnswerElement loadParseVendorConfigurationAnswerElement(
          NetworkSnapshot snapshot) throws IOException {
    return deserializeObject(
            getParseVendorConfigurationAnswerElementPath(snapshot),
            ParseVendorConfigurationAnswerElement.class);
  }

  @Override
  public void storeParseVendorConfigurationAnswerElement(
          ParseVendorConfigurationAnswerElement parseVendorConfigurationAnswerElement,
          NetworkSnapshot snapshot)
          throws IOException {
    serializeObject(
            parseVendorConfigurationAnswerElement,
            getParseVendorConfigurationAnswerElementPath(snapshot));
  }

  @Override
  public boolean hasParseVendorConfigurationAnswerElement(NetworkSnapshot snapshot)
          throws IOException {
    return objectExists(toKeyPath(getParseVendorConfigurationAnswerElementPath(snapshot)));
  }

  @Override
  public void deleteParseVendorConfigurationAnswerElement(NetworkSnapshot snapshot)
          throws IOException {
    deleteObjects(getParseVendorConfigurationAnswerElementPath(snapshot));
  }

  @Nonnull
  @Override
  public Map<String, VendorConfiguration> loadVendorConfigurations(NetworkSnapshot snapshot)
          throws IOException {
    _logger.info("\n*** DESERIALIZING VENDOR CONFIGURATION STRUCTURES ***\n");
    _logger.resetTimer();
    Map<Path, String> namesByPath = new TreeMap<>();
    Path serializedVendorConfigPath = getVendorConfigurationsPath(snapshot);
    if (!isDirectory(serializedVendorConfigPath)) {
      return ImmutableSortedMap.of();
    }

    try (Stream<S3Object> objs = listFolder(serializedVendorConfigPath)) {
      objs.forEach(obj -> {
        String name = StringUtils.substringAfterLast(obj.key(), "/");
        namesByPath.put(fromKeyPath(obj.key()), name);
      });
    }

    Map<String, VendorConfiguration> vendorConfigurations =
            deserializeObjects(namesByPath, VendorConfiguration.class);
    _logger.printElapsedTime();
    return vendorConfigurations;
  }

  private @Nonnull Path getVendorConfigurationsPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_VENDOR_SPECIFIC_CONFIG_DIR);
  }

  @Override
  public void storeVendorConfigurations(
          Map<String, VendorConfiguration> vendorConfigurations, NetworkSnapshot snapshot)
          throws IOException {
    Map<Path, VendorConfiguration> output = new HashMap<>();
    Path outputPath = getVendorConfigurationsPath(snapshot);
    vendorConfigurations.forEach(
            (name, vc) -> {
              Path currentOutputPath = outputPath.resolve(name);
              output.put(currentOutputPath, vc);
            });
    serializeObjects(output);
  }

  @Override
  public void deleteVendorConfigurations(NetworkSnapshot snapshot) throws IOException {
    deleteObjects(getVendorConfigurationsPath(snapshot));
  }

  @MustBeClosed
  @Nonnull
  @Override
  public Stream<String> listInputHostConfigurationsKeys(NetworkSnapshot snapshot)
          throws IOException {
    return listSnapshotInputObjectKeys(snapshot)
            .filter(key -> keyInDir(key, BfConsts.RELPATH_HOST_CONFIGS_DIR));
  }

  @MustBeClosed
  @Nonnull
  @Override
  public Stream<String> listInputNetworkConfigurationsKeys(NetworkSnapshot snapshot)
          throws IOException {
    return listSnapshotInputObjectKeys(snapshot)
            .filter(key -> keyInDir(key, BfConsts.RELPATH_CONFIGURATIONS_DIR));
  }

  @MustBeClosed
  @Nonnull
  @Override
  public Stream<String> listInputAwsMultiAccountKeys(NetworkSnapshot snapshot) throws IOException {
    return listSnapshotInputObjectKeys(snapshot)
            .filter(key -> keyInDir(key, RELPATH_AWS_ACCOUNTS_DIR));
  }

  @MustBeClosed
  @Nonnull
  @Override
  public Stream<String> listInputAwsSingleAccountKeys(NetworkSnapshot snapshot) throws IOException {
    return listSnapshotInputObjectKeys(snapshot)
            .filter(key -> keyInDir(key, BfConsts.RELPATH_AWS_CONFIGS_DIR));
  }

  @MustBeClosed
  @Nonnull
  @Override
  public Stream<String> listInputCheckpointManagementKeys(NetworkSnapshot snapshot)
          throws IOException {
    return listSnapshotInputObjectKeys(snapshot)
            .filter(key -> keyInDir(key, BfConsts.RELPATH_CHECKPOINT_MANAGEMENT_DIR));
  }

  @MustBeClosed
  @Nonnull
  @Override
  public Stream<String> listInputSonicConfigsKeys(NetworkSnapshot snapshot) throws IOException {
    return listSnapshotInputObjectKeys(snapshot)
            .filter(key -> keyInDir(key, BfConsts.RELPATH_SONIC_CONFIGS_DIR));
  }

  private @Nonnull Path getParseVendorConfigurationAnswerElementPath(NetworkSnapshot snapshot) {
    return getSnapshotOutputDir(snapshot.getNetwork(), snapshot.getSnapshot())
            .resolve(RELPATH_PARSE_ANSWER_PATH);
  }

  @Override
  public void runGarbageCollection() throws IOException {
    // Go back GC_SKEW_ALLOWANCE, so we under-approximate data to delete. This helps minimize race
    // conditions with in-progress or queued work.
    Instant expungeBeforeDate = Instant.now().minus(GC_SKEW_ALLOWANCE);

    // Iterate over network dirs directly so we get both extant and deleted networks.
    // TODO: this is apparently an expensive call, should find a better way to check if this folder exists
    // besides using listobjectsv2
    if (isDirectory(getNetworksDir())) {
      Set<String> extantNetworkIds = listResolvedIds(NetworkId.class);
      ImmutableList.Builder<Path> dirsToExpunge = ImmutableList.builder();
      List<String> networkDirPrefixes = listCommonPrefixes(getNetworksDir());
      networkDirPrefixes.stream()
              .map(prefix -> fromKeyPath(prefix))
              .map(networkDir -> new NetworkId(networkDir.getFileName().toString()))
              .forEach(
                networkId -> {
                  try {
                    if (extantNetworkIds.contains(networkId.toString())) {
                      dirsToExpunge.addAll(getSnapshotDirsToExpunge(networkId, expungeBeforeDate));
                    } else {
                      if (canExpungeNetwork(networkId, expungeBeforeDate)) {
                        dirsToExpunge.add(getNetworkDir(networkId));
                      }
                    }
                  } catch (IOException e) {
                    _logger.errorf(
                            "Failed to garbage collect network with ID '%s': %s",
                            networkId, Throwables.getStackTraceAsString(e));
                    LOGGER.error(
                            String.format("Failed to garbage collect network with ID %s", networkId),
                            e);
                  }
                });

      dirsToExpunge
              .build()
              .forEach(
                      dir -> {
                        try {
                          deleteObjects(dir);
                        } catch (SdkException e) {
                          _logger.errorf(
                                  "Failed to expunge directory '%s': %s",
                                  dir, Throwables.getStackTraceAsString(e));
                          LOGGER.error(String.format("Failed to expunge directory %s", dir), e);
                        }
                      });
    }
  }

  private List<Path> getSnapshotDirsToExpunge(NetworkId networkId, Instant expungeBeforeDate)
          throws IOException {
    // the directory may not exist if snapshots were never initialized in the network
    if (!isDirectory(getSnapshotsDir(networkId))) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<Path> snapshotDirsToDelete = ImmutableList.builder();
    Set<String> extantSnapshotIds = listResolvedIds(SnapshotId.class, networkId);

    List<String> snapshotDirs = listCommonPrefixes(getSnapshotsDir(networkId));
    snapshotDirs.stream()
            .map(key -> fromKeyPath(key))
            .map(snapshotDir -> new SnapshotId(snapshotDir.getFileName().toString()))
            .forEach(
                    snapshotId -> {
                      if (canExpungeSnapshot(networkId, snapshotId, expungeBeforeDate)) {
                        snapshotDirsToDelete.add(getSnapshotDir(networkId, snapshotId));
                      }
                    });
    return snapshotDirsToDelete.build();
  }

  /**
   * Returns if it is safe to delete this network's folder, based on the last modified times of
   * itself, its subdirs, and its snapshots.
   */
  @VisibleForTesting
  boolean canExpungeNetwork(NetworkId networkId, Instant expungeBeforeDate) throws IOException {
    Path networkDir = getNetworkDir(networkId);
    try (Stream<S3Object> subdirs = listFolder(networkDir)) {
      if (!canExpunge(expungeBeforeDate, subdirs)) {
        return false;
      }
    }

    // the directory may not exist if snapshots were never initialized in the network
    if (!isDirectory(getSnapshotsDir(networkId))) {
      return true;
    }
    List<String> snapshotFolders = listCommonPrefixes(getSnapshotsDir(networkId));
    return snapshotFolders.stream()
            .map(prefix -> fromKeyPath(prefix))
            .map(snapshotDir -> new SnapshotId(snapshotDir.getFileName().toString()))
            .allMatch(snapshotId -> canExpungeSnapshot(networkId, snapshotId, expungeBeforeDate));
  }

  /**
   * Returns if it is safe to delete this snapshot's folder, based on the last modified time of its
   * input, output, and answers.
   */
  @VisibleForTesting
  boolean canExpungeSnapshot(
          NetworkId networkId, SnapshotId snapshotId, Instant expungeBeforeDate) {
    Stream<S3Object> snapshotDirStream = listFolder(getSnapshotDir(networkId, snapshotId));
    Stream<S3Object> snapshotInputDirStream = listFolder(getSnapshotInputObjectsDir(networkId, snapshotId));
    Stream<S3Object> snapshotOutputDirStream = listFolder(getSnapshotOutputDir(networkId, snapshotId));
    Stream<S3Object> answerDirStream = listFolder(getAnswersDir(networkId, snapshotId));
    Stream<S3Object> allDirs = Stream.concat(
            snapshotDirStream,
            Stream.concat(snapshotInputDirStream,
            Stream.concat(snapshotOutputDirStream, answerDirStream)));

    return canExpunge(expungeBeforeDate, allDirs);
  }

  /**
   * Returns if all paths in {@code pathStream} have a last modified time less than the {@code
   * expungeBeforeDate}..
   */
  private boolean canExpunge(Instant expungeBeforeDate, Stream<S3Object> objStream) {
    return objStream
            .map(obj -> obj.lastModified())
            .allMatch(lmTime -> lmTime.compareTo(expungeBeforeDate) < 0);
  }
}
