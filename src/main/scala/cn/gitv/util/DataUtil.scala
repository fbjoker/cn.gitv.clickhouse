package cn.gitv.util

import java.text.SimpleDateFormat

/**
  * Created by s1363 on 2019/3/11.
  */
object DataUtil {

    def getByte() = {
        val data = (Math.random()*1000000).toByte
        data
    }

    def getLong() ={
        val data = (Math.random()*1000000).toLong
        data
    }

    def getInt() ={
        val data = (Math.random()*10000).toInt
        data
    }

    def getShort() ={
        val data = (Math.random()*1000).toShort
        data
    }


    def getString() = {
        var fieldName = ""
        val fieldNameSize = (Math.round(Math.random()*4)+4).toInt
        for(i <- 1 to fieldNameSize){
            fieldName += ((Math.round(Math.random()*58)+65).toChar)
        }
        fieldName

    }
    def getMac() = {
        "192.168.131.120"
    }
    def getTime() = {
        val time:Long= System.currentTimeMillis()
        val newtime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
        newtime.toString
    }

    def getFloat() = {
        val data = (Math.random()*10).formatted("%.2f").toFloat
        data
    }

    def getDouble() = {
        val data = (Math.random()*10).formatted("%.2f").toDouble
        data
    }

    def matchType(str: String) = {
        str match {
            case "Int" => getInt()
            case "Long" => getLong()
            case "Short" => getShort()
            case "Byte" => getByte()
            case "String" => getString()
            case "Float" => getFloat()
            case "Double" => getDouble()
        }
    }
}
