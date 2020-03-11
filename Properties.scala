package source.dev.utils

import org.apache.commons.configuration2.Configuration

trait Properties {
  def getMap(config: Configuration, key: String): Map[String, String] = {
    val sub: Configuration = config.subset(key)
    getMap(sub, sub.getKeys)
  }

  /**
    * Recursively extract all of the keys and values from the sub-config into a Map
    * @param conf
    * @param keys
    * @return
    */
  def getMap(conf: Configuration, keys: java.util.Iterator[String]): Map[String, String] = {
    if (keys.hasNext) {
      val key = keys.next()
      getMap(conf, keys) + (key -> conf.getStringArray(key).mkString(","))
    } else {
      Map()
    }
  }

  def getRequiredStringArrayProperty(config: Configuration, key: String): Array[String] = {
    val value = config.getStringArray(key)
    if (value == null || value.isEmpty) {
      throw new IllegalArgumentException(s"Required property $key not set in app.properties!")
    }
    value
  }

  def getRequiredProperty(config: Configuration, key: String): String = {
    val value = config.getString(key)
    if (nullOrBlank(value)) {
      throw new IllegalArgumentException(s"Required property $key not set in app.properties!")
    }
    value
  }

  def getLongProperty(config: Configuration, key: String): Long = {
    val value = config.getLong(key)
    if (value == null) {
      throw new IllegalArgumentException(s"Required property $key not set in app.properties!")
    }
    value
  }

  def nullOrBlank(s : String) : Boolean = {
    s == null || s.isEmpty
  }
}
