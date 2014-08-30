package edu.hnu.cn.app;

import java.io.FileNotFoundException;

import edu.hnu.cg.framework.*;
import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.datablocks.DoubleConverter;
import edu.hnu.cg.graph.userdefine.GraphConfig;

public class PageRankHandler implements Handler<Double, Float, Double> {

	@Override
	public Double compute(int from, int to, Double val, Double msg,int superstep) {
		if(superstep == 0)
			return (1.0/GraphConfig.totalVertices);
		
		return 0.85 * msg + 0.15/GraphConfig.totalVertices ;
	}

	@Override
	public byte[] genMessage(int from, int to, Double val, Float weight, MsgConverter<Double> msgConv) {
		byte[] msg = new byte[msgConv.sizeOf()];
		msgConv.setFrom(from, msg);
		msgConv.setTo(to, msg);
		msgConv.setValue(msg, val);
		return msg;
	}

	public static void main(String[] args) throws FileNotFoundException {
		DoubleConverter dc = new DoubleConverter();
		String graphFile = args[0];
		String format = args[1];
		
		Graph<Double, Float, Double> graph = new Graph<>(graphFile, format, null, dc, null, null);
		
		Manager<Double,Float,Double> mgr = new Manager<>(graph, new PageRankHandler(), new Combiner<Double>() {
			@Override
			public void combine(byte[] combine, byte[] msg, MsgConverter<Double> converter) {
				int from = converter.getFrom(msg);
				int to = converter.getTo(msg);
				
				double one = converter.getValue(combine);
				double two = converter.getValue(msg);
				double sum = one + two;
				
				converter.setFrom(from, combine);
				converter.setTo(to,combine);
				converter.setValue(combine, sum);
				
			}
			
		}, dc, dc, null);
		
		mgr.start();
	}

}


