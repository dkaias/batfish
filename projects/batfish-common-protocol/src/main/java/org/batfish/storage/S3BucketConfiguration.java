package org.batfish.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@JsonIgnoreProperties(ignoreUnknown = true)
@ParametersAreNonnullByDefault
public class S3BucketConfiguration implements Serializable{
  @Nonnull private final String _bucketName;
  @Nonnull private final String _regionName;
  @Nonnull private final String _profileName;

  public String getBucketName() {
    return _bucketName;
  }

  public String getRegionName() {
    return _regionName;
  }

  public String getProfileName() {
    return _profileName;
  }

  @JsonCreator
  private static S3BucketConfiguration create(
      @Nonnull @JsonProperty("bucketName") String bucketName,
      @Nonnull @JsonProperty("regionName") String regionName,
      @Nonnull @JsonProperty("profileName") String profileName) {
    return new S3BucketConfiguration(bucketName, regionName, profileName);
  }

  public S3BucketConfiguration(
      String bucketName,
      String regionName,
      String profileName) {
    _bucketName = bucketName;
    _regionName = regionName;
    _profileName = profileName;
  }
}
