package com.twitter.rowz

import org.specs.Specification
import com.twitter.util.Eval
import java.io.File

import config.{Rowz => RowzConfig}

object ConfigValidationSpec extends Specification {

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
