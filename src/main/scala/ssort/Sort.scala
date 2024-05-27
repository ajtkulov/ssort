package ssort

import sort.FileUtils
import ssort.Main.blockSize

import scala.collection.mutable.ArrayBuffer
import java.io._

case class Batch(file: RandomAccessFile, offset: Long, size: Long, outputFileName: String, idx: Int) {
  var len = blockSize
  val newLines = ArrayBuffer[Int](-1)
  var newAr: Array[Int] = null
  var buffer = new Array[Byte](blockSize)
  var isEnd = false
  var bytesRead: Int = 0

  def read() = {
    file.seek(offset)
    bytesRead = file.read(buffer)
    if (offset + bytesRead == size) {
      isEnd = true
    }
  }

  def init() = {
    for {i <- 0 until bytesRead} {
      if (buffer(i) == 10) {
        newLines.append(i)
      }
    }

    if (!isEnd) {
      len = newLines.last
    }
  }

  def internalSort() = {
    newAr = Array.range(0, newLines.size - 1).sortWith { case (l, r) =>
      val ls = newLines(l + 1) - newLines(l)
      val rs = newLines(r + 1) - newLines(r)
      val msize = Math.min(ls, rs)

      var i = 0
      var isres = false
      var res = false
      val ll = newLines(l) + 1
      val rr = newLines(r) + 1
      while (i < msize && !isres) {
        val li: Int = buffer(ll + i) & 0xFF
        val ri: Int = buffer(rr + i) & 0xFF
        (li, ri) match {
          case (a, b) if a < b => res = true; isres = true
          case (a, b) if a > b => res = false; isres = true
          case _ => ()
        }
        i = i + 1
      }
      res
    }
  }

  def write() = {

    import java.io.FileOutputStream
    val fos = new FileOutputStream(s"$outputFileName.$idx")
    for {idx <- 0 until newLines.size - 1} {
      fos.write(buffer, newLines(newAr(idx)) + 1, newLines(newAr(idx) + 1) - newLines(newAr(idx)) - 1)
      fos.write(10)
    }
    fos.close()
  }

  def customFinalize() = {
    buffer = null
  }
}

object Main extends App {
  //  val blockSize = 1000000009
  val blockSize = 10009

  def handleInput(inputFile: String): Unit = {

    val size: Long = FileUtils.fileSize(inputFile)
    val file = new RandomAccessFile(inputFile, "r")

    var idx = 0
    var offset = 0L
    var isEnd: Boolean = false

    while (!isEnd) {
      val b = Batch(file, offset, size, inputFile, idx)
      idx = idx + 1

      b.read()
      isEnd = b.isEnd
      b.init()
      offset = offset + b.len + 1

      b.internalSort()
      b.write()
      System.gc()
    }
  }

  handleInput("s")
}
