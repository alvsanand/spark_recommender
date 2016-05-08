package es.alvsanand.spark_recommender.parser

import java.io._
import java.net.URL

import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.io.IOUtils

/**
  * Created by alvsanand on 7/05/16.
  */
object DatasetDownloader {
  private val DATASET_URL = "http://times.cs.uiuc.edu/~wang296/Data/LARA/Amazon/AmazonReviews.zip"

  private val DATASET_NAME = "%s/AmazonReviews.zip"
  private val TMP_DATASET_NAME = "%s/AmazonReviews.zip.tmp"
  private val FINAL_DATASET_NAME = "%s/AmazonReviews"

  @throws(classOf[IOException])
  def download(dst: String): Unit = {
    val fileName = DATASET_NAME.format(dst)
    val tmpFileName = TMP_DATASET_NAME.format(dst)
    val finalDstName = FINAL_DATASET_NAME.format(dst)

    val file = new File(fileName)
    val finalDstDile = new File(finalDstName)

    if (!finalDstDile.exists() || finalDstDile.list().size == 0) {
      if (!file.exists()) {
        println("Downloading Dataset file[%s] to %s".format(DATASET_URL, fileName))

        var in = None: Option[InputStream]
        var out = None: Option[OutputStream]

        try {
          val dstDir = new File(dst)

          !dstDir.exists() && dstDir.mkdir()

          val tmpFile = new File(tmpFileName)

          val dataset = new URL(DATASET_URL)

          in = Some(dataset.openStream())
          out = Some(new FileOutputStream(tmpFile))

          IOUtils.copy(in.get, out.get)

          tmpFile.renameTo(file)

          println("Downloaded Dataset file[%s] to %s".format(DATASET_URL, fileName))
        } finally {
          if (in.isDefined) in.get.close
          if (out.isDefined) out.get.close
        }
      }
      else {
        println("Dataset file[%s] already downloaded to %s".format(DATASET_URL, fileName))
      }

      println("Unzziping Dataset file[%s] to %s".format(fileName, finalDstName))

      unzipFile(fileName, finalDstName)

      println("Unzziped Dataset file[%s] to %s".format(fileName, finalDstName))
    }
    else {
      println("Dataset[%s] already exists in %s".format(DATASET_URL, finalDstName))
    }
  }

  @throws(classOf[IOException])
  private def unzipFile(zipFileName: String, outputFolder: String): Unit = {
    val buffer = new Array[Byte](1024)

    val folder = new File(outputFolder)
    if (!folder.exists()) {
      folder.mkdir()
    }

    val zipFile = new ZipFile(zipFileName)

    try {
      val entries = zipFile.getEntries

      while (entries.hasMoreElements()) {
        val entry = entries.nextElement()
        val entryDestination = new File(folder, entry.getName())

        if (entry.isDirectory()) {
          entryDestination.mkdirs()
        } else {
          entryDestination.getParentFile().mkdirs()
          val in = zipFile.getInputStream(entry)
          val out = new FileOutputStream(entryDestination)

          IOUtils.copy(in, out)
          IOUtils.closeQuietly(in)
          IOUtils.closeQuietly(out)
        }
      }
    } finally {
      zipFile.close()
    }
  }
}
