package main

import (
	"github.com/urfave/cli"
	"testing"
	"io/ioutil"
	"flag"
	"errors"
)


func TestTopic(t *testing.T) {
	cases := []struct {
		testArgs        []string
		skipFlagParsing bool
		skipArgReorder  bool
		expectedErr     error
	}{
		// Test normal "not ignoring flags" flow
		{[]string{"test-cmd", "topic", "--addr", "127.0.0.1:9092",  "list"},
			false, false,
			nil},
	}

	for _, c := range cases {
		app := cli.NewApp()
		app.Writer = ioutil.Discard
		set := flag.NewFlagSet("test", 0)
		set.Parse(c.testArgs)

		context := cli.NewContext(app, set, nil)

		command := cli.Command{
			Name:            "test-cmd",
			Aliases:         []string{"tc"},
			Usage:           "this is for testing",
			Description:     "testing",
			Action:          topicCommand,
			SkipFlagParsing: c.skipFlagParsing,
			SkipArgReorder:  c.skipArgReorder,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
				cli.StringFlag{Name: "t, topic", Usage: "kafka集群的指定topic"},
				cli.IntFlag{Name: "p", Usage: "kakfa某个topic的分区"},
			},
		}

		err := command.Run(context)
		if err != nil {
			t.Log("run topic test failed ", err)
		}
	}

}

func TestBroker(t *testing.T) {
	cases := []struct {
		testArgs        []string
		skipFlagParsing bool
		skipArgReorder  bool
		expectedErr     error
	}{
		// Test normal "not ignoring flags" flow
		{[]string{"test-cmd", "brokers", "addr", "127.0.0.1:9092"},
		 false, false,
		 errors.New("flag provided but not defined: -break")},
	}

	for _, c := range cases {
		app := cli.NewApp()
		app.Writer = ioutil.Discard
		set := flag.NewFlagSet("test", 0)
		set.Parse(c.testArgs)

		context := cli.NewContext(app, set, nil)

		command := cli.Command{
			Name:            "test-cmd",
			Aliases:         []string{"tc"},
			Usage:           "this is for testing",
			Description:     "testing",
			Action:          brokerCommand,
			SkipFlagParsing: c.skipFlagParsing,
			SkipArgReorder:  c.skipArgReorder,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
			},
		}

		err := command.Run(context)
		if err != nil {
			t.Log("run brokers test failed ", err)
		}
	}
}

func TestPartions(t *testing.T) {
	cases := []struct {
		testArgs        []string
		skipFlagParsing bool
		skipArgReorder  bool
		expectedErr     error
	}{
		// Test normal "not ignoring flags" flow
		{[]string{"test-cmd", "partitions", "--addr", "127.0.0.1:9092", "-t", "beats"},
		 false, false,
		 errors.New("flag provided but not defined: -break")},
	}

	for _, c := range cases {
		app := cli.NewApp()
		app.Writer = ioutil.Discard
		set := flag.NewFlagSet("test", 0)
		set.Parse(c.testArgs)

		context := cli.NewContext(app, set, nil)

		command := cli.Command{
			Name:            "test-cmd",
			Aliases:         []string{"tc"},
			Usage:           "this is for testing",
			Description:     "testing",
			Action:          partitionsCommand,
			SkipFlagParsing: c.skipFlagParsing,
			SkipArgReorder:  c.skipArgReorder,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
				cli.StringFlag{Name: "t,topic", Usage: "指定kafka的topic"},
			},
		}

		err := command.Run(context)
		if err != nil {
			t.Log("run partition test failed ", err)
		}
	}
}

func TestProcudeConsum(t *testing.T) {
	cases := []struct {
		testArgs        []string
		skipFlagParsing bool
		skipArgReorder  bool
		expectedErr     error
	}{
		// Test normal "not ignoring flags" flow
		{[]string{"test-cmd", "produce", "--addr", "127.0.0.1:9092", "-t", "beats"},
		 false, false,
		 errors.New("flag provided but not defined: -break")},
	}

	for _, c := range cases {
		app := cli.NewApp()
		app.Writer = ioutil.Discard
		set := flag.NewFlagSet("test", 0)
		set.Parse(c.testArgs)

		context := cli.NewContext(app, set, nil)

		command := cli.Command{
			Name:            "test-cmd",
			Aliases:         []string{"tc"},
			Usage:           "this is for testing",
			Description:     "testing",
			Action:          produceCommand,
			SkipFlagParsing: c.skipFlagParsing,
			SkipArgReorder:  c.skipArgReorder,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "addr, ip", Usage: "kafka集群的任意一个地址"},
				cli.StringFlag{Name: "t, topic", Usage: "指定topic"},
				cli.StringFlag{Name: "m, message", Usage: "消息内容"},
				cli.IntFlag{Name: "c, count", Usage: "生产几份消息"},
			},
		}

		err := command.Run(context)
		if err != nil {
			t.Log("run produce test failed ", err)
		}
	}
}