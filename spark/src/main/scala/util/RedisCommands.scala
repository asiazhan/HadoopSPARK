package util

trait RedisCommands {
  def set(key: String, value: String): String

  def get(key: String): String

  def existe(key: String): Boolean

  def expire(key: String, seconds: Int):Long

  def pexpire(key: String, milliseconds: Long):Long
}
