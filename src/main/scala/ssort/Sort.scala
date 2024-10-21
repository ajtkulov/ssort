package ssort

import sort.FileUtils

import scala.collection.mutable.ArrayBuffer
import java.io._
import scala.concurrent.{Await, ExecutionContextExecutorService, Future}
import scala.concurrent.duration.Duration
import java.io.FileOutputStream
import org.rogach.scallop._

import java.util.concurrent.atomic.AtomicInteger

case class Batch(file: RandomAccessFile, offset: Long, size: Long, outputFileName: String, idx: Int, blockSize: Int) {
  var len = blockSize
  @volatile var newLines = ArrayBuffer[Int](-1)
  @volatile var newAr: Array[Int] = null
  @volatile var buffer = new Array[Byte](blockSize)
  var isEnd = false
  var bytesRead: Int = 0

  def read(): Unit = {
    file.seek(offset)
    bytesRead = file.read(buffer)
    if (offset + bytesRead == size) {
      isEnd = true
    }
  }

  def init(): Unit = {
    for {i <- 0 until bytesRead} {
      if (buffer(i) == 10) {
        newLines.append(i)
      }
    }

    if (!isEnd) {
      len = newLines.last
    }
  }

  def internalSort(): Unit = {
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

  def write(): Unit = {
    val fos = new FileOutputStream(s"$outputFileName.$idx")
    for {idx <- 0 until newLines.size - 1} {
      fos.write(buffer, newLines(newAr(idx)) + 1, newLines(newAr(idx) + 1) - newLines(newAr(idx)) - 1)
      fos.write(10)
    }
    fos.close()
  }

  def customFinalize() = {
    file.close()
    buffer = null
    newAr = null
    newLines = null
  }
}

case class StringWrap(ar: Array[Byte], offset: Int, len: Int, isLastInBlock: Boolean, index: Int)

object StringWrapOrdering {
  val ordering = new Ordering[StringWrap] {
    override def compare(x: StringWrap, y: StringWrap): Int = {
      val msize = Math.min(x.len, y.len)
      var i = 0
      var isres = false
      var res = 0

      while (i < msize && !isres) {
        val li: Int = x.ar(x.offset + i) & 0xFF
        val ri: Int = y.ar(y.offset + i) & 0xFF
        (li, ri) match {
          case (a, b) if a < b => res = 1; isres = true
          case (a, b) if a > b => res = -1; isres = true
          case _ => ()
        }
        i = i + 1
      }

      res
    }
  }
}

class FileIterator(val fileName: String, val offset: Int = 0, val bufferSize: Int = 10000000, val index: Int) {
  val size = FileUtils.fileSize(fileName)
  val file = new RandomAccessFile(fileName, "r")
  file.seek(offset)
  var buffer: Array[Byte] = new Array[Byte](bufferSize)
  val bytesRead = file.read(buffer)
  val newLines = ArrayBuffer[Int](-1)
  val isReadTillEnd: Boolean = offset.toLong + bytesRead.toLong == size

  def nextChunk(): Vector[StringWrap] = {
    var pos = offset
    while (pos < offset + bytesRead) {
      if (buffer(pos - offset) == 10) {
        newLines.append(pos - offset)
      }
      pos = pos + 1
    }

    val linesCount = newLines.size - 1
    newLines.sliding(2).zipWithIndex.toVector.map { case (p, idd) =>
      StringWrap(buffer, p.head + 1, p.last - p.head - 1, idd == linesCount - 1, index)
    }.filter(_.len > 1)
  }
}

class MergeSort(inputFileName: String, chunks: Int, outputFileName: String) {

  val outputStream = new FileOutputStream(outputFileName)
  val heap = scala.collection.mutable.PriorityQueue[StringWrap]()(StringWrapOrdering.ordering)
  val files = (0 until chunks).map(x => s"$inputFileName.$x").toVector
  val fileMap = new Array[FileIterator](chunks)

  def init(): Unit = {
    files.zipWithIndex.foreach { case (f, idx) =>
      fileMap(idx) = new FileIterator(f, 0, index = idx)
      val chunks = fileMap(idx).nextChunk()
      assert(chunks.count(_.isLastInBlock) == 1)
      chunks.foreach { str =>
        heap.enqueue(str)
      }
    }
  }

  def sort(): Unit = {
    while (heap.nonEmpty) {
      val head = heap.dequeue()

      if (head.isLastInBlock) {
        val idx = head.index
        if (fileMap(idx) != null) {
          if (!fileMap(idx).isReadTillEnd) {
            val curFileIterator = fileMap(idx)
            fileMap(idx) = new FileIterator(curFileIterator.fileName, curFileIterator.offset + curFileIterator.newLines.last, curFileIterator.bufferSize, idx)
            val chunks = fileMap(idx).nextChunk()

            chunks.foreach { str =>
              heap.enqueue(str)
            }
          } else {
            fileMap(idx) = null
          }
        }
      }

      outputStream.write(head.ar, head.offset, head.len)
      outputStream.write(10)
    }

    outputStream.close()
  }

}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String](required = true)
  val output = opt[String](required = true)
  val blocksize = opt[Int]()
  val threads = opt[Int]()
  verify()
}

object Main extends App {
  def sortFile(inputFile: String, blockSize: Int)(implicit ec: ExecutionContextExecutorService): Int = {
    val futures: ArrayBuffer[Future[Unit]] = ArrayBuffer[Future[Unit]]()
    val size: Long = FileUtils.fileSize(inputFile)

    var idx = 0
    var offset = 0L
    var isEnd: Boolean = false

    val readCnt: AtomicInteger = new AtomicInteger(0)
    val doneCnt: AtomicInteger = new AtomicInteger(0)

    while (!isEnd) {
      while (readCnt.get() - doneCnt.get() > 6) {
        println(s"read: ${readCnt.get()} - done: ${doneCnt.get()}")
        Thread.sleep(1000)
      }

      val b = Batch(new RandomAccessFile(inputFile, "r"), offset, size, inputFile, idx, blockSize)
      idx = idx + 1

      b.read()
      isEnd = b.isEnd
      b.init()
      offset = offset + b.len + 1

      readCnt.incrementAndGet()

      val f = Future {
        b.internalSort()
        b.write()
        b.customFinalize()
        doneCnt.incrementAndGet()
        System.gc()
      }
      futures.append(f)
    }

    for (f <- futures) {
      Await.result(f, Duration.Inf)
      System.gc()
    }

    futures.size
  }

  def cleanUp(filesToDelete: Vector[String]) = {
    filesToDelete.foreach(file => FileUtils.delete(file))
  }

  override def main(args: Array[String]) {
    val conf = new Conf(args)

    val blockSize: Int = conf.blocksize.getOrElse(500000009).toInt
    val input = conf.input.get.get
    val output = conf.output.get.get
    implicit val ec = ExecutionPool.createExecutionContext(conf.threads.getOrElse(4))

    val chunks: Int = sortFile(input, blockSize)

    val m = new MergeSort(input, chunks, output)
    m.init()
    m.sort()
    cleanUp(m.files)
  }
}
