package uishow

import (
	"errors"
	"fmt"
	ui "github.com/gizak/termui" // <- ui shortcut, optional
	"github.com/urfave/cli"
	log "github.com/wupeaking/logrus"
	kafka "github.com/wupeaking/sarama"
	"strings"
	"time"
)

var renderCallBacks = make([]func() error, 0)

func registerRenderCallBacks(cb func() error) {
	renderCallBacks = append(renderCallBacks, cb)
}

func renderInit() {
	err := ui.Init()
	if err != nil {
		panic(err)
	}

	// handle key q pressing
	ui.Handle("/sys/kbd/q", func(ui.Event) {
		// press q to quit
		ui.StopLoop()
	})

	ui.Handle("/sys/kbd/C-x", func(ui.Event) {
		// handle Ctrl + x combination
	})

	ui.Handle("/sys/kbd", func(ui.Event) {
		// handle all other key pressing
		ui.StopLoop()
		ui.Close()
	})
	ui.Handle("/timer/1s", func(e ui.Event) {
		for _, cb := range renderCallBacks {
			cb()
		}
	})
}

//此块区域的左上角和有下角坐标
func renderPar(tx, ty, bx, by int) (int, int, int, int) {
	//p := ui.NewPar("有边框文本")
	//p.Height = 3
	//p.Width = 50
	//p.TextFgColor = ui.ColorWhite
	////
	//p.BorderLabel = "介绍"
	//p.BorderFg = ui.ColorCyan

	// 再创建一个无边框
	p2 := ui.NewPar("     kafkainfo 是一个简单的调试工具（按任意键退出）")
	p2.Border = false
	p2.X = tx
	p2.Y = by + 1
	p2.Height = 3
	p2.TextFgColor = ui.ColorCyan
	p2.Width = 60

	ui.Render(p2) // feel free to call Render, it's async and non-block

	registerRenderCallBacks(func() error {
		p2.Border = !p2.Border
		p2.BorderLabel = time.Now().String()
		ui.Render(p2)
		return nil
	})

	return p2.X, p2.Y, p2.X + p2.Width, p2.Y + p2.Height

}

// 渲染所有的broker 返回 此块区域的左上角和有下角坐标
func renderBrokerInfo(brokerInfo map[string]string, tx, ty, bx, by int) (int, int, int, int) {
	addrs := make([]string, 0)
	ids := make([]string, 0)

	for addr, id := range brokerInfo {
		addrs = append(addrs, addr)
		ids = append(ids, id)
	}

	addrList := ui.NewList()
	addrList.Items = addrs
	addrList.BorderFg = ui.ColorYellow
	addrList.ItemFgColor = ui.ColorCyan
	addrList.BorderLabel = "kafka broker addr list"
	addrList.Height = len(addrs) + 3
	addrList.Width = 50
	addrList.Y = by + 1
	addrList.X = tx

	idList := ui.NewList()
	idList.Items = ids
	idList.BorderFg = ui.ColorYellow
	idList.ItemFgColor = ui.ColorCyan
	idList.BorderLabel = "kafka broker id list"
	idList.Height = len(addrs) + 3
	idList.Width = 50
	idList.Y = addrList.Y
	idList.X = addrList.X + addrList.Width

	ui.Render(addrList, idList)

	return addrList.X, addrList.Y, addrList.X + addrList.Width, addrList.Y + addrList.Height
}

// 渲染topic信息
// topic名称 | 分区数量 每个分区的leader
func renderTopicInfo(topicinfo map[string]int, partsLeader map[string][]string, tx, ty, bx, by int) (int, int, int, int) {

	infos := make([]string, 0)
	infos = append(infos, "主题 | 分区数量 | 分区leader地址")
	for topic, num := range topicinfo {
		info := fmt.Sprintf("%s | %d | %s", topic, num, strings.Join(partsLeader[topic], " "))
		infos = append(infos, info)
	}
	infoList := ui.NewList()
	infoList.Items = infos
	infoList.BorderFg = ui.ColorYellow
	infoList.ItemFgColor = ui.ColorCyan
	infoList.BorderLabel = "kafka broker topics info list"
	infoList.Height = len(infos) + 3
	infoList.Width = 100
	infoList.Y = by + 1
	infoList.X = tx

	ui.Render(infoList)
	return infoList.X, infoList.Y, infoList.X + infoList.Width, infoList.Y + infoList.Height
}

func renderLoop() {
	ui.Loop()
}

func getAllTopics(kafkaCli kafka.Client) []string {
	topics, err := kafkaCli.Topics()

	if err != nil {
		return nil
	} else {
		return topics
	}
	//brokers := kafkaCli.Brokers()
	//
	//for i, broker := range brokers {
	//	log.WithField("index:", i).Info("id: ", broker.ID, " addr: ", broker.Addr())
	//}
	//return nil
}

// 获取所有的topic对应的分区数量
func getTopicsInfo(kafkaCli kafka.Client, topics []string) map[string]int {
	info := make(map[string]int)
	for _, topic := range topics {
		parts, err := kafkaCli.Partitions(topic)
		if err != nil {
			info[topic] = 0
		} else {
			info[topic] = len(parts)
		}
	}
	return info
}

// 获取每个topic的每个分区的leader
func getPartsLeader(kafkaCli kafka.Client, topicInfo map[string]int) map[string][]string {
	partLeader := make(map[string][]string)
	for topic, parts := range topicInfo {
		partLeader[topic] = make([]string, 0)
		p := 0
		for p < parts {
			broker, e := kafkaCli.Leader(topic, int32(p))
			if e != nil {
				partLeader[topic] = append(partLeader[topic], "")
			} else {
				partLeader[topic] = append(partLeader[topic], broker.Addr())
			}
			p += 1
			if p > 3 {
				// 只显示前三个
				break
			}
		}
	}
	return partLeader
}

// 获取所有的broker key: addr value id
func getAllBrokerINfo(kafkaCli kafka.Client) map[string]string {
	brkInfo := make(map[string]string, 0)
	brokers := kafkaCli.Brokers()
	for _, b := range brokers {
		brkInfo[b.Addr()] = fmt.Sprintf("%d", b.ID())
	}
	return brkInfo
}

func UIshowCommand(c *cli.Context) error {

	addr := c.String("addr")
	if addr == "" {
		log.Error(`必须传递kafka地址 使用"kakfainfo uishow -h" 查看帮助信息`)
		return errors.New("kafka地址为空")
	}
	// 创建一个kafka客户端
	kafkaCli, err := kafka.NewClient(strings.Split(addr, ","), nil)
	if err != nil {
		log.Error("连接kafka失败: ", err)
		return err
	}

	// 获取所有topic
	topics := getAllTopics(kafkaCli)
	// 获取所有topic对应的分区数量
	topicsinfos := getTopicsInfo(kafkaCli, topics)
	// 获取每个topic的每个分区的leader
	partsLeader := getPartsLeader(kafkaCli, topicsinfos)
	// 获取所有的broker
	brokerInfo := getAllBrokerINfo(kafkaCli)
	defer kafkaCli.Close()
	//// 初始化渲染
	renderInit()

	//// 渲染一个标题
	tx, ty, bx, by := renderPar(0, 0, 0, 0)

	//// 渲染broker信息
	tx, ty, bx, by = renderBrokerInfo(brokerInfo, tx, ty, bx, by)
	////  渲染主题详情
	renderTopicInfo(topicsinfos, partsLeader, tx, ty, bx, by)
	renderLoop()

	return nil
}
