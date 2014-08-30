package edu.hnu.cn.app;

import java.io.FileNotFoundException;

import edu.hnu.cg.framework.Combiner;
import edu.hnu.cg.framework.Handler;
import edu.hnu.cg.framework.Manager;
import edu.hnu.cg.framework.MsgConverter;
import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.datablocks.DoubleConverter;
import edu.hnu.cg.graph.datablocks.FloatConverter;
import edu.hnu.cg.graph.preprocessing.EdgeProcessor;

public class SSSPHandler implements Handler<Double, Float, Double> {

	@Override
	public Double compute(int from, int to, Double val, Double msg,int superstep) {
		return val > msg ? msg : val;
	}

	@Override
	public byte[] genMessage(int from, int to, Double val, Float weight, MsgConverter<Double> msgConv) {
		byte[] msg = new byte[msgConv.sizeOf()];
		msgConv.setFrom(from, msg);
		msgConv.setTo(to, msg);
		msgConv.setValue(msg, val + weight);
		return msg;
	}

	public static void main(String[] args) throws FileNotFoundException {
		
		DoubleConverter dc = new DoubleConverter();
		FloatConverter fc = new FloatConverter();
		String graphFile = args[0];
		String format = args[1];

		Graph<Double, Float, Double> graph = new Graph<>(graphFile, format, fc, dc, null, new EdgeProcessor<Float>() {

			@Override
			public Float receiveEdge(int from, int to, String token) {
				return token == null ? 0.0f : Float.parseFloat(token);
			}
		});

		System.out.println("star..");
		Manager<Double, Float, Double> mgr = new Manager<>(graph, new SSSPHandler(), new Combiner<Double>() {
			@Override
			public void combine(byte[] combine, byte[] msg, MsgConverter<Double> converter) {
				double one = converter.getValue(combine);
				double two = converter.getValue(msg);
				
				if (one > two) {
					System.arraycopy(msg, 0, combine, 0, converter.sizeOf());
				}

			}

		}, dc, dc, null);

		mgr.start();
	}

}

class SSSPType{
	int preVertexID;
	double val;
}
