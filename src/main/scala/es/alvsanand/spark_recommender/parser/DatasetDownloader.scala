package es.alvsanand.spark_recommender.parser

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

    if(!finalDstDile.exists()){
      if(!file.exists()){
        println("Downloading Dataset file[%s] to %s".format(DATASET_URL, fileName))

        var in = None: Option[InputStreamReader]
        var out = None: Option[OutputStreamWriter]

        try {
          val dstDir = new File(dst)

          !dstDir.exists() && dstDir.mkdir()

          val tmpFile = new File(tmpFileName)

          val dataset = new URL(DATASET_URL)

          in = Some(new InputStreamReader(dataset.openStream()))
          out = Some(new FileWriter(tmpFile))
          var c = 0
          while ({c = in.get.read; c != -1}) {
            out.get.write(c)
          }

          tmpFile.renameTo(file)

          println("Downloaded Dataset file[%s] to %s".format(DATASET_URL, fileName))
        } finally {
          if (in.isDefined) in.get.close
          if (out.isDefined) out.get.close
        }
      }
      else{
        println("Dataset file[%s] already downloaded to %s".format(DATASET_URL, fileName))
      }

      println("Unzziping Dataset file[%s] to %s".format(fileName, finalDstName))

      unzipFile(fileName, finalDstName)

      println("Unzziped Dataset file[%s] to %s".format(fileName, finalDstName))
    }
    else{
      println("Dataset[%s] already exists in %s".format(DATASET_URL, finalDstName))
    }
  }

  @throws(classOf[IOException])
  private def unzipFile(zipFile: String, outputFolder: String): Unit = {
    val buffer = new Array[Byte](1024)

    try {
      val folder = new File(outputFolder);
      if (!folder.exists()) {
        folder.mkdir();
      }

      val zis: ZipInputStream = new ZipInputStream(new FileInputStream(zipFile));
      var ze: ZipEntry = zis.getNextEntry();

      while (ze != null) {
        val fileName = ze.getName();
        val newFile = new File(outputFolder + File.separator + fileName);

        new File(newFile.getParent()).mkdirs();

        val fos = new FileOutputStream(newFile);

        var len: Int = zis.read(buffer);

        while (len > 0) {

          fos.write(buffer, 0, len)
          len = zis.read(buffer)
        }

        fos.close()
        ze = zis.getNextEntry()
      }

      zis.closeEntry()
      zis.close()
    }
  }
}
