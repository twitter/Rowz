package com.twitter.rowz

import org.specs.Specification
import com.twitter.util.Eval
import java.io.File

import config.{Rowz => RowzConfig}

object ConfigValidationSpec extends Specification {
  def setEnvVarForTest(key: String, value: String) {
    val env      = System.getenv()
    val mapClass = classOf[java.util.Collections].getDeclaredClasses() filter {
      _.getName == "java.util.Collections$UnmodifiableMap"
    } head

    val field = mapClass.getDeclaredField("m")
    field.setAccessible(true)

    field.get(env).asInstanceOf[java.util.Map[String,String]].put(key, value)
  }

  setEnvVarForTest("DB_USERNAME", "root")
  setEnvVarForTest("DB_PASSWORD", "pass")

  "Configuration Validation" should {
    "test.scala" in {
      val conf = Eval[RowzConfig](new File("config/test.scala"))
      conf mustNot beNull
    }

    "development.scala" in {
      val conf = Eval[RowzConfig](new File("config/development.scala"))
      conf mustNot beNull
    }
  }
}
