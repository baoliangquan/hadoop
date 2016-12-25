package com.storm.demo;

import java.io.FileWriter;import java.io.IOException;import java.util.Map;
import java.util.Random;import java.util.UUID;

import backtype.storm.Config;import backtype.storm.StormSubmitter;import backtype.storm.generated.StormTopology;import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;import backtype.storm.topology.base.BaseBasicBolt;import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomWordSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;

	//模拟一些数据
	String[] words = {"iphone","xiaomi","mate","sony","sumsung","moto","meizu"};

	//不断地往下一个组件发送tuple消息
	//这里面是该spout组件的核心逻辑
	public void nextTuple() {

		//可以从kafka消息队列中拿到数据,简便起见，我们从words数组中随机挑选一个商品名发送出去
		Random random = new Random();
		int index = random.nextInt(words.length);

		//通过随机数拿到一个商品名
		String godName = words[index];


		//将商品名封装成tuple，发送消息给下一个组件
		collector.emit(new Values(godName));

		//每发送一个消息，休眠500ms
		Utils.sleep(500);


	}

	//初始化方法，在spout组件实例化时调用一次
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this.collector = collector;


	}

	//声明本spout组件发送出去的tuple中的数据的字段名
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("orignname"));

	}

public static class SuffixBolt extends BaseBasicBolt{

	FileWriter fileWriter = null;


	//在bolt组件运行过程中只会被调用一次
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		try {
			fileWriter = new FileWriter("/home/hadoop/stormoutput/"+UUID.randomUUID());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}



	//该bolt组件的核心处理逻辑
	//每收到一个tuple消息，就会被调用一次
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		//先拿到上一个组件发送过来的商品名称
		String upper_name = tuple.getString(0);
		String suffix_name = upper_name + "_itisok";


		//为上一个组件发送过来的商品名称添加后缀

		try {
			fileWriter.write(suffix_name);
			fileWriter.write("\n");
			fileWriter.flush();

		} catch (IOException e) {
			throw new RuntimeException(e);
		}



	}




	//本bolt已经不需要发送tuple消息到下一个组件，所以不需要再声明tuple的字段
	public void declareOutputFields(OutputFieldsDeclarer arg0) {


	}

} /**
 * 组织各个处理组件形成一个完整的处理流程，就是所谓的topology(类似于mapreduce程序中的job)
 * 并且将该topology提交给storm集群去运行，topology提交到集群后就将永无休止地运行，除非人为或者异常退出
 * @author duanhaitao@itcast.cn
 *
 */
public static class TopoMain {


	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		//将我们的spout组件设置到topology中去
		//parallelism_hint ：4  表示用4个excutor来执行这个组件
		//setNumTasks(8) 设置的是该组件执行时的并发task数量，也就意味着1个excutor会运行2个task
		builder.setSpout("randomspout", new RandomWordSpout(), 4).setNumTasks(8);

		//将大写转换bolt组件设置到topology，并且指定它接收randomspout组件的消息
		//.shuffleGrouping("randomspout")包含两层含义：
		//1、upperbolt组件接收的tuple消息一定来自于randomspout组件
		//2、randomspout组件和upperbolt组件的大量并发task实例之间收发消息时采用的分组策略是随机分组shuffleGrouping
		builder.setBolt("upperbolt", new UpperBolt(), 4).shuffleGrouping("randomspout");

		//将添加后缀的bolt组件设置到topology，并且指定它接收upperbolt组件的消息
		builder.setBolt("suffixbolt", new SuffixBolt(), 4).shuffleGrouping("upperbolt");

		//用builder来创建一个topology
		StormTopology demotop = builder.createTopology();


		//配置一些topology在集群中运行时的参数
		Config conf = new Config();
		//这里设置的是整个demotop所占用的槽位数，也就是worker的数量
		conf.setNumWorkers(4);
		conf.setDebug(true);
		conf.setNumAckers(0);


		//将这个topology提交给storm集群运行
		StormSubmitter.submitTopology("demotopo", conf, demotop);

	}
}public static class UpperBolt extends BaseBasicBolt{


	//业务处理逻辑
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		//先获取到上一个组件传递过来的数据,数据在tuple里面
		String godName = tuple.getString(0);

		//将商品名转换成大写
		String godName_upper = godName.toUpperCase();

		//将转换完成的商品名发送出去
		collector.emit(new Values(godName_upper));

	}



	//声明该bolt组件要发出去的tuple的字段
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("uppername"));
	}

}}
