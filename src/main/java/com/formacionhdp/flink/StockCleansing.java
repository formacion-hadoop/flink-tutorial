package com.formacionhdp.flink;

import java.util.Date;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;

public class StockCleansing {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		String outputFile = params.getRequired("output");
		final String symbol = params.getRequired("symbol");

		DataSet<Tuple5<String, String, Integer, Double, Date>> output = env.readTextFile(input)
				.filter(input1 -> input1.split(",").length == 5)
				.map(new MapFunction<String, Tuple5<String, String, Integer, Double, Date>>() {

					@Override
					public Tuple5<String, String, Integer, Double, Date> map(String input) throws Exception {
						String[] fields = input.split(",");
						return new Tuple5<String, String, Integer, Double, Date>(fields[0], fields[1],
								new Integer(fields[2]), new Double(fields[3]), new Date(new Long(fields[4])));
					}
				}).filter(new FilterFunction<Tuple5<String, String, Integer, Double, Date>>() {

					@Override
					public boolean filter(Tuple5<String, String, Integer, Double, Date> input) throws Exception {
						return input.f0.equals(symbol);
					}
				});

		output.writeAsCsv(outputFile);
		output.print();

	}
}
