import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/*移动项目实时统计
实时读取kafka的数据，并更新偏移量到mysql中
    //将统计结果保存到Redis中，并将偏移量更新到mysql中

 */

object cms_SparkingStreaming {
  // 屏蔽日志
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("cms_SparkingStreaming")
    // 时间设计：批次时间应该大于这个批次处理完的总的花费（total delay）时间
    val streamingContext = new StreamingContext(conf, Seconds(2))

    val load = ConfigFactory.load()
    // 创建kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> load.getString("kafka.broker.list"),
      "group.id" -> load.getString("kafka.group.id"),
      "auto.offset.reset" -> "smallest"
    )
    //获取kafka主题
    val tp = load.getString("kafka.topics").split(",").toSet


    //加载mysql配置信息,获取mysql中的偏移量
    DBs.setup()
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      SQL("select * from streaming_offset_24 where groupid=?").bind(load.getString("kafka.group.id")).map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
      }).toList().apply()
    }.toMap //DB.readOnly返回的是一个list，把他变为的tomap


    //从kafka中读取数据，从mysql中获取偏移量
    // 从数据库中获取到当前的消费到的偏移量位置 -- 从该位置接着往后消费
    //假设程式第一次启动
    var kafkaStream: InputDStream[(String, String)] = null
    if (fromOffsets.size == 0) {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, tp)
    } else {
      var checkedOffset = Map[TopicAndPartition, Long]()
      //校验参数是否过期
      val kafkaCluster = new KafkaCluster(kafkaParams)
      //获取kafka集群的最早偏移量
      val earliestLeaderOffsets: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)
      if (earliestLeaderOffsets.isRight) {
        val map: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = earliestLeaderOffsets.right.get
        //开始集群的偏移量与mysql保存的偏移量作对比
        checkedOffset = fromOffsets.map(f => {
          val clusterOffset: Long = map.get(f._1).get.offset
          if (f._2 > clusterOffset) {
            f
          } else {
            (f._1, clusterOffset)
          }
        })
      }
      // 程序菲第一次启动
      val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
      //第一个string传输到kafka的key的类型，第二个string传输到kafka的value的类型StringDecoder是字符串解码器
      // 第三第四个string是：mm.key(), mm.message()返回的类型
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamingContext, kafkaParams, checkedOffset, messageHandler)
    }

    /**
      * receiver 接受数据是在Executor端 cache -- 如果使用的窗口函数的话，没必要进行cache, 默认就是cache， WAL ；
      * 如果采用的不是窗口函数操作的话，你可以cache, 数据会放做一个副本放到另外一台节点上做容错
      * direct 接受数据是在Driver端
      */
    //处理数据，根据业务需求
    kafkaStream.foreachRDD(rdd => {
      // rdd.foreach(println)
      //获取偏移量信息，并打印出来看看
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //       offsetRanges.foreach(osr=>{
      //         println(s"${osr.topic} ${osr.partition} ${osr.fromOffset} ${osr.untilOffset}")
      //       })
      /**
        * 实时报表 -- 业务概况
        * 1)统计全网的充值订单量, 充值金额, 充值成功率及充值平均时长.
        */
      val baseData = rdd.map(t => JSON.parseObject(t._2))
        //充值类的serviceName的值为reChargeNotifyReq
        .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
        .map(jsObj => {
          val result = jsObj.getString("bussinessRst")
          //业务结果，0000为成功
          //订单的充值金额
          val fee: Double = if (result.equals("0000")) jsObj.getDouble("chargefee") else 0
          val isSucc: Double = if (result.equals("0000")) 1 else 0
          //计算平均时长指标
          val receiveTime = jsObj.getString("receiveNotifyTime")
          val startTime = jsObj.getString("requestId")
          // 消耗的时间
          val costime = if (result.equals("0000")) Utils.AvgUtil.caculateRqt(startTime, receiveTime) else 0
          //获取省份数据
          val pCode = jsObj.getString("provinceCode")
          ("A-" + startTime.substring(0, 8), startTime.substring(0, 10), List[Double](1, isSucc, fee, costime.toDouble), pCode, startTime.substring(0, 12))
        })
      //对上述指标统计存入Redis中
      baseData.map(t => (t._1, t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map (x => x._1 + x._2)
      }).foreachPartition(itr => {
        val client = Utils.RedisUtil.getJedisClient()

        itr.foreach(tp => {
          client.hincrBy(tp._1, "total", tp._2(0).toLong)
          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
          client.hincrByFloat(tp._1, "money", tp._2(2))
          client.hincrBy(tp._1, "timer", tp._2(3).toLong)
          //设置在Redis中保留数据的时长
          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })

      // 每小时的充值业务办理趋势指标
      baseData.map(t => ("B-" + t._2, t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map (x => x._1 + x._2)
      }).foreachPartition(itr => {

        val client = Utils.RedisUtil.getJedisClient()
        itr.foreach(tp => {
          // B-2017111816
          client.hincrBy(tp._1, "total", tp._2(0).toLong)
          client.hincrBy(tp._1, "succ", tp._2(1).toLong)

          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })
      // 每个省份每小时的充值成功数据
      baseData.map(t => ((t._2, t._4), t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map (x => x._1 + x._2)
      }).foreachPartition(itr => {
        val client = Utils.RedisUtil.getJedisClient()

        itr.foreach(tp => {
          client.hincrBy("P-" + tp._1._1.substring(0, 8), tp._1._2, tp._2(1).toLong)
          client.expire("P-" + tp._1._1.substring(0, 8), 60 * 60 * 24 * 2)
        })
        client.close()
      })

      // 每分钟的数据分布情况统计,实时统计每分钟的充值笔数和充值金额
      baseData.map(t => ("C-" + t._5, t._3)).reduceByKey((list1, list2) => {
        (list1 zip list2) map (x => x._1 + x._2)
      }).foreachPartition(itr => {

        val client = Utils.RedisUtil.getJedisClient()
        itr.foreach(tp => {
          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
          client.hincrByFloat(tp._1, "money", tp._2(2))
          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })


      // 记录偏移量,并保存偏移量到mysql中
      offsetRanges.foreach(osr => {
        DB.autoCommit { implicit session =>
          //REPLACE INTO 是首先删除数据再更新数据
          SQL("REPLACE INTO streaming_offset_24(topic, groupid, partitions, offset) VALUES(?,?,?,?)")
            .bind(osr.topic, load.getString("kafka.group.id"), osr.partition, osr.untilOffset).update().apply()
        }
      })


    })


    //启动程式，等待程式结束
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
