import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.{Configurations, Parameters}
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler
import org.apache.commons.configuration2.{Configuration, FileBasedConfiguration, PropertiesConfiguration}

/**S
  * Configuration for the Spark Engine which gets loaded from a properties file.
  * Must be serializable since it is used by spark so this has been broken out into a case class which is
  * serializable by default.
  */
class FilteredStreamingConfig(config_filename: String) extends ApplicationConfig {

  val NINETY_DAYS = 90 * 86400 * 1000 // in milliseconds
  // do not serialize the config objects, only used for loading
  @transient val configs: Configurations = new Configurations()
  @transient val params = new Parameters()
  @transient val builder = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
    .configure(params.properties()
    .setFileName(config_filename)
    .setListDelimiterHandler(new DefaultListDelimiterHandler(','))
    )
  @transient val config: Configuration = builder.getConfiguration
  val sparkMaster: String = getRequiredProperty(config, "spark.master")
  // var because during unit tests we have to change the quorum
  val sparkPartitionSize: Long = config.getLong("spark.partition.size", 134217728)
  val sparkPartitionMemoryMultiplier: Int = config.getInt("spark.partition.memory.multiplier", 3)
  val debugDisableCryptoVault: Boolean = config.getBoolean("debug.disable.cryptovault", false)
  val cryptoLabel: String = getRequiredProperty(config, "crypto.label")
  val hdfsUri: String = getRequiredProperty(config, "hdfs.uri")
  val avroSchemasDirectory: String = config.getString("avro.schemas.directory")
  val jobReloadCheckInterval: Int = config.getInt("job.reload.check.interval")
}

class ApplicationConfig extends RequiredProperties with Serializable
