package main

import (
	"errors"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli"
	ui "github.com/wupeaking/kafkainfo/uishow"
	log "github.com/wupeaking/logrus"
	kafka "github.com/wupeaking/sarama"
	kafkaCluster "github.com/wupeaking/sarama-cluster"
)

// 获取kafka的所有topic
func topicCommand(c *cli.Context) error {
	// 获取kafka地址
	addr := c.String("addr")
	// 获取第一个参数
	arags := c.Args().Get(0)
	if addr == "" {
		log.Error(`必须传递kafka地址 使用"kakfainfo topic -h" 查看帮助信息`)
		return errors.New("kafka地址为空")
	}

	// 创建一个kafka客户端
	kafkaCli, err := kafka.NewClient(strings.Split(addr, ","), nil)
	if err != nil {
		log.Error("连接kafka失败: ", err)
		return err
	}
	defer kafkaCli.Close()

	switch arags {
	case "list":
		topics, err := kafkaCli.Topics()
		if err != nil {
			log.Error("获取kafka的topics失败: ", err)
			return err
		}
		log.Info("所有的topics: ", topics)
		return nil
	case "leader":
		topic := c.String("t")
		if topic == "" {
			log.Error("topic 参数必须指定")
		}
		part := c.Int("p")
		broker, err := kafkaCli.Leader(topic, int32(part))
		if err != nil {
			log.Error("获取分区leader失败", "part:", part, "topic: ", topic, "err:", err)
			return errors.New("获取leader失败")
		} else {
			log.Info("topic:", topic, " part:", part, " addr: ", broker.Addr(), " id:", broker.ID())
			return nil
		}

	case "":
		log.Info("kafkainfo topic --addr localhost:9092 list 显示所有存在的topic")
		log.Info("kafkainfo topic --addr localhost:9092 -t beats -p 0 leader 显示指定topic的某个分区的leader")
	}
	return nil
}

// 获取kafka的所有分区
func partitionsCommand(c *cli.Context) error {
	addr := c.String("addr")
	if addr == "" {
		log.Error(`必须传递kafka地址 使用"kakfainfo partitions -h" 查看帮助信息`)
		return errors.New("kafka地址为空")
	}

	//指定topic的分区
	topic := c.String("topic")
	if topic == "" {
		log.Error(`topic 是必须参数 使用"kakfainfo partitions -h" 查看帮助信息`)
		return errors.New("need topic ")
	}

	// 创建一个kafka客户端
	kafkaCli, err := kafka.NewClient(strings.Split(addr, ","), nil)
	if err != nil {
		log.Error("连接kafka失败: ", err)
		return err
	}
	defer kafkaCli.Close()

	parts, err := kafkaCli.Partitions(topic)
	if err != nil {
		log.Error("获取kafka的分区失败: ", err)
		return err
	}

	log.Info(topic, "的分区情况: ", parts)
	return nil
}

// 获取所有的broker
func brokerCommand(c *cli.Context) error {
	addr := c.String("addr")
	if addr == "" {
		log.Error(`必须传递kafka地址 使用"kakfainfo partitions -h" 查看帮助信息`)
		return errors.New("kafka地址为空")
	}

	// 创建一个kafka客户端
	kafkaCli, err := kafka.NewClient(strings.Split(addr, ","), nil)
	if err != nil {
		log.Error("连接kafka失败: ", err)
		return err
	}
	defer kafkaCli.Close()

	brokers := kafkaCli.Brokers()

	for i, broker := range brokers {
		log.WithField("index:", i).Info("id: ", broker.ID, " addr: ", broker.Addr())
	}
	return nil
}

// 生产消息命令
func produceCommand(c *cli.Context) error {
	addr := c.String("addr")
	topic := c.String("topic")
	count := c.Int("c")

	msg := c.String("m")

	if addr == "" || topic == "" {
		log.Error("addr, topic参数必须设置")
		return errors.New("addr, topic参数必须设置")
	}

	if msg == "" {
		msg = "this is kakfainfo test "
	}
	if count <= 0 {
		count = 1
	}

	config := kafka.NewConfig()
	config.ClientID = "kafkainfo"
	// 同步模式的生产者 必须设置为TRUE
	config.Producer.Return.Successes = true
	// 生产者发往哪一个分区 默认是通过key的hash值 现在改为随机
	config.Producer.Partitioner = kafka.NewRandomPartitioner
	produce, e := kafka.NewSyncProducer(strings.Split(addr, ","), config)

	if e != nil {
		log.Error("生成消息失败", e)
		return errors.New("生成消息失败")
	}
	defer produce.Close()

	sumCount := 0
	for count > 0 {
		count--
		proMsg := &kafka.ProducerMessage{
			Topic:     topic,
			Partition: int32(-1),
			Key:       kafka.StringEncoder(time.Now().String()),
		}
		proMsg.Value = kafka.ByteEncoder(msg)
		part, offset, err := produce.SendMessage(proMsg)
		if err != nil {
			log.Error("生产消息失败")
			break
		} else {
			sumCount++
			log.Info("生产消息信息: ", "part: ", part, " offset:", offset)
		}
	}
	log.Info("生产消息成功个数: ", sumCount)
	return nil
}

// 消费kafka的内容
func consumCommand(c *cli.Context) error {
	addr := c.String("addr")
	topic := c.String("topic")
	count := c.Int("c")
	forever := c.Bool("f")

	if addr == "" || topic == "" {
		log.Error("addr, topic参数必须设置")
		return errors.New("addr, topic参数必须设置")
	}
	if count <= 0 && !forever {
		log.Error("count或者forever必须设置其中一个", "count表示消费count个内容后退出",
			"forever 表示一直消费直到按下Ctrl-C后退出")
		return errors.New("参数错误")
	}

	config := kafkaCluster.NewConfig()
	// 一秒同步一下偏移值
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = kafka.OffsetOldest
	config.ClientID = "kafkainfo"
	//
	config.Consumer.Return.Errors = true // 是否返回错误通知
	config.Group.Return.Notifications = false

	// 创建一个消费者
	consumer, err := kafkaCluster.NewConsumer(strings.Split(addr, ","), "kafkainfo",
		[]string{topic}, config)
	if err != nil {
		log.Error("新建消费者失败", err)
		return errors.New("新建消费者失败")
	}
	defer consumer.Close()
	go func() {
		for err := range consumer.Errors() {
			log.Error("消费消息出现异常: ", err)
		}
	}()
	sig := make(chan os.Signal)
	// 监控ctrl-c
	signal.Notify(sig, syscall.SIGINT)

	sumSuc := 0
	if count > 0 {
		goto countLabel
	} else {
		goto foreverLabel
	}

countLabel:
	// 开始消费消息
	for {
		select {
		case msg := <-consumer.Messages():
			sumSuc++
			log.Info("主题: ", msg.Topic, " 分区: ", msg.Partition, " 偏移量: ",
				msg.Offset, " key: ", msg.Key, " value:", string(msg.Value))
			consumer.MarkOffset(msg, "") // mark message as processed
			if sumSuc >= count {
				break
			}
		case <-sig:
			log.Info("总共消费消息数量:", sumSuc)
			return nil
		}
	}

foreverLabel:
	for {
		select {
		case msg := <-consumer.Messages():
			sumSuc++
			log.Info("主题: ", msg.Topic, " 分区: ", msg.Partition, " 偏移量: ",
				msg.Offset, " key: ", string(msg.Key), " value: ", string(msg.Value))
			consumer.MarkOffset(msg, "") // mark message as processed
		case <-sig:
			log.Info("总共消费消息数量:", sumSuc)
			return nil
		}

	}

}

func main() {

	log.SetLevel(log.DebugLevel)
	// 获取kafka存在的所有topic

	app := cli.NewApp()
	app.Name = "kafkainfo"
	app.Usage = "kafkainfo是一个简单的调试kafka的命令行工具"
	app.Version = "0.0.1"
	//app.Action = func(c *cli.Context) error {
	//	println("当没有匹配的子命令时，执行这个函数")
	//	return nil
	//}
	// 初始化几个子命令
	// 1. 获取kaka的topics
	topic := cli.Command{Name: "topics", Usage: "kafka的topic信息", Action: topicCommand,
		Flags: []cli.Flag{
			cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
			cli.StringFlag{Name: "t, topic", Usage: "kafka集群的指定topic"},
			cli.IntFlag{Name: "p", Usage: "kakfa某个topic的分区"},
		},
	}
	// 2. 分区
	partitions := cli.Command{Name: "partitions", Usage: "kafka的指定topic的分区", Action: partitionsCommand,
		Flags: []cli.Flag{
			cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
			cli.StringFlag{Name: "t,topic", Usage: "指定kafka的topic"},
		}}

	// brokers
	brokers := cli.Command{Name: "brokers", Usage: "kafka的指定所有brokers", Action: brokerCommand,
		Flags: []cli.Flag{
			cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
		}}

	// produce 生产消息
	produce := cli.Command{Name: "produce", Usage: "向kafka生成若干消息", Action: produceCommand,
		Flags: []cli.Flag{
			cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
			cli.StringFlag{Name: "t, topic", Usage: "指定topic"},
			cli.StringFlag{Name: "m, message", Usage: "消息内容"},
			cli.IntFlag{Name: "c, count", Usage: "生产几份消息"},
		}}

	// 消费消息
	consum := cli.Command{Name: "consum", Usage: "消费kafka消息", Action: consumCommand,
		Flags: []cli.Flag{
			cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
			cli.StringFlag{Name: "t, topic", Usage: "指定topic"},
			cli.IntFlag{Name: "c, count", Usage: "消费几份消息"},
			cli.BoolFlag{Name: "f, forerver", Usage: "一直消费 直到按下Ctrl-C"},
		}}

	// 图形显示kafka信息
	uishow := cli.Command{Name: "uishow", Usage: "在shell上图形化显示kafka概述信息", Action: ui.UIshowCommand,
		Flags: []cli.Flag{
			cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
		}}
	app.Commands = []cli.Command{topic, partitions, brokers, produce, consum, uishow}
	app.Run(os.Args)
}
