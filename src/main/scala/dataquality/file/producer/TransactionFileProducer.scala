package dataquality.file.producer

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

object TransactionFileProducer {

  def main(args: Array[String]) {
    val accountNumbers = List("123-ABC-789", "123-DEF-788", "123-GHI-787", "123-JKL-786")
    val descriptions = List("Drug Store", "Grocery Store", "Electronics", "Park", "Gas", "Books", "Movies", "Misc")

    val linesCount = args.headOption.map(_.toInt).getOrElse(1000)
		val file = args.lift(1).getOrElse("transactions")

		var writer: BufferedWriter = null
    try {
      writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
      println(s"Begin generating $linesCount transactions into a file $file")
      for (i <- 1 to linesCount) {
        val txStr = produceTransaction(i.toString)
        writer.write(txStr + "\n")
      }
      println(s"Completed generating $linesCount transactions into a file $file")
    } finally {
      if (writer != null) {
        writer.close()
      }
    }

		def produceTransaction(id: String): String = {
			val accountNumber = accountNumbers(scala.util.Random.nextInt(accountNumbers.size))
			val description = descriptions(scala.util.Random.nextInt(descriptions.size))
			val currentTs = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date())
			val txAmount = math.floor((scala.util.Random.nextInt(5000) + scala.util.Random.nextDouble) * 100) / 100

			s"""{"id":"$id","transaction_timestamp":"$currentTs","account":"$accountNumber","amount":"$txAmount","category":"$description"}"""
		}
  }


}

