import java.util
import scala.io.Source

import scala.collection.mutable
import scala.math.{max, min}

class Preprocessor(path: String) {
  val defaultPath = "dictionary.txt"
  var dictionary: scala.collection.mutable.HashMap[String, Boolean] = null

  def this() {
    this("dictionary.txt")
    this.dictionary = scala.collection.mutable.HashMap[String, Boolean]()
    for (line <- Source.fromFile(path).getLines) {
      this.dictionary.put(line, true)
    }

  }

  //https://gist.github.com/tixxit/1246894/e79fa9fbeda695b9e8a6a5d858b61ec42c7a367d
  def levenshteinDistance[A](a: Iterable[A], b: Iterable[A]) =
    ((0 to b.size).toList /: a) ((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last

  def closestWordFromDictionary(word: String): String = {
    if (dictionary.contains(word)) {
      word
    } else {
      var closestWord = ""
      var minDistance = 10000
      dictionary.foreach(tuple => {
        if (minDistance > levenshteinDistance(word, tuple._1)) {
          minDistance = levenshteinDistance(word, tuple._1)
          closestWord = tuple._1
        }
      })
      closestWord
    }
  }
  def removeRedundantLetters(word: String): String ={
    val regex ="(\\b\\w*?)(\\w)\\2{2,}(\\w*)"
    return word.replaceAll(regex,"$1$2$2$3")
  }

  def process(word: String): String = {
    return closestWordFromDictionary(removeRedundantLetters(word.toLowerCase()))
  }
}

object Preprocessor {
  private var _instance: Preprocessor = null

  def instance(): Preprocessor = {
    if (_instance == null)
      _instance = new Preprocessor()
    _instance
  }
}