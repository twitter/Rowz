import sbt._
import Process._
import com.twitter.sbt.{StandardServiceProject, DefaultRepos, CompileThriftJava}


class RowzProject(info: ProjectInfo) extends StandardServiceProject(info)
with DefaultRepos
with CompileThriftJava {

  override def filterScalaJars = false
  val scalaTooles = "org.scala-lang" % "scala-compiler" % "2.8.1"

  val gizzard   = "com.twitter" % "gizzard" % "2.1.6"

  val asm       = "asm"                     % "asm"          % "1.5.3" % "test"
  val cglib     = "cglib"                   % "cglib"        % "2.1_3" % "test"
  val hamcrest  = "org.hamcrest"            % "hamcrest-all" % "1.1"   % "test"
  val objenesis = "org.objenesis"           % "objenesis"    % "1.1"   % "test"
  val jmock     = "org.jmock"               % "jmock"        % "2.4.0" % "test"
  val specs     = "org.scala-tools.testing" % "specs_2.8.1"  % "1.6.6" % "test"
}
