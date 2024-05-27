package sort

import java.io._
import java.nio.file.{Files, Paths}
import scala.reflect.io
import scala.reflect.io.Directory

/**
 * Utility object for File access.
 */
object FileUtils {

  type FileName = String
  type Dir = String

  def withFile[A](fileName: FileName)(func: PrintWriter => A): Unit = {
    val file = new File(fileName)
    val write = new PrintWriter(file)
    try {
      func(write)
    } finally {
      write.close()
    }
  }

  /**
   * Directory list
   *
   * @param dir dir
   * @return only filenames, i.e. "/tmp/1/2/3" -> "3"
   */
  def list(dir: Dir): List[FileName] = {
    filesInDir(dir).map(_.name).toList
  }

  def traverse(dir: String): Vector[FileName] = {
    val names: Vector[FileName] = Directory(dir).dirs.toVector.map(_.toAbsolute.name).flatMap(d => traverse(s"$dir/$d"))
    fullList(dir).toVector ++ names
  }

  /**
   * Full directory list
   *
   * @param dir dir
   * @return full path filenames, i.e. "/tmp/1/2/3"
   */
  def fullList(dir: Dir): List[FileName] = {
    list(dir).map(fileName => s"$dir/$fileName")
  }

  def fromFile(filePath: FileName, encoding: String = "iso-8859-1"): Iterator[String] = scala.io.Source.fromFile(filePath, encoding).getLines

  def readFile(filePath: FileName, encoding: String = "iso-8859-1"): String = fromFile(filePath, encoding).mkString("\n")

  def readBinaryFile(fileName: FileName): Array[Byte] = {
    Files.readAllBytes(Paths.get(fileName))
  }

  def writeBinaryFile(fileName: FileName, array: Array[Byte]): Unit = {
    Files.write(Paths.get(fileName), array)
  }

  def filesInDir(dir: Dir, fileNameFilter: (FileName => Boolean) = (x => true)): Array[io.File] = {
    Directory(dir).files.toArray.filter(file => fileNameFilter(file.name)).sortBy(x => x.name)
  }

  // scalastyle:off regex
  def write(fileName: FileName, iterator: Iterator[String]): Unit = {
    withFile(fileName) { output =>
      iterator.foreach(line => output.println(line))
    }
  }

  // scalastyle:on regex

  def write(fileName: FileName, value: String): Unit = {
    write(fileName, Iterator.single(value))
  }

  def write(fileName: FileName, array: Array[Byte], offset: Int = 0, len: Int = -1): Unit = {
    import java.io.FileOutputStream
    val fos = new FileOutputStream(fileName)
    val l = if (len == -1) {
      array.length
    } else {
      len
    }
    fos.write(array, offset, l)
    fos.close()
  }


  def write(fileName: FileName, stream: InputStream): Unit = {
    Files.copy(stream, new java.io.File(fileName).toPath)
  }

  def copyFile(srcPath: String, destPath: String): Unit = {
    val src = new File(srcPath)
    val dest = new File(destPath)
    new FileOutputStream(dest).getChannel.transferFrom(
      new FileInputStream(src).getChannel, 0, Long.MaxValue)
  }

  def exist(path: String): Boolean = {
    new java.io.File(path).exists
  }

  def delete(fileName: FileName): Boolean = {
    new File(fileName).delete()
  }

  def deleteNonEmptyDir(dir: Dir): Boolean = {
    filesInDir(dir).foreach(x => delete(x.path))
    new Directory(new File(dir)).delete()
  }

  def fileSize(fileName: FileName): Long = {
    new File(fileName).length()
  }

  def appendLine(fileName: FileName, value: String): Unit = {
    val fileWriter = new FileWriter(fileName, true)
    try {
      fileWriter.write(value)
      fileWriter.write("\n")
    } finally {
      fileWriter.close()
    }
  }

  def readSplit[T](fileName: FileName)(f: Array[String] => T): Iterator[T] = {
    scala.io.Source.fromFile(fileName, "UTF-8").getLines().map { line =>
      f(line.split("\t"))
    }
  }
}
