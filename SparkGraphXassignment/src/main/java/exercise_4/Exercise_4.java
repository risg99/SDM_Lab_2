package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;

import java.util.List;

public class Exercise_4 {

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		// reading the edges
		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.read().option("delimiter", "\t").schema(edges_schema).csv("src/main/resources/wiki-edges.txt");
		edges.show();
		// reading the vertices
		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build()),
		});
		Dataset<Row> vertices = sqlCtx.read().option("delimiter", "\t").schema(vertices_schema).csv("src/main/resources/wiki-vertices.txt");
		vertices.show();

		// running the page rank algorithm
		System.out.println("Running the page rank algorithm.");
		GraphFrame gf = GraphFrame.apply(vertices, edges);

		System.out.println("Reset Probability chosen: 0.01");
		System.out.println("Maximum Iterations before the algorithm converges: 5");
		PageRank pRank = gf.pageRank().resetProbability(0.01).maxIter(5);
		pRank.run().vertices().select("id", "pagerank").show();
	}
}
