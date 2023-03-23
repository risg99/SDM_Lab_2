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
                // Edge read		
		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.read().option("delimiter", "\t").schema(edges_schema).csv("src/main/resources/wiki-edges.txt");
		edges.show();
		// vertice read
		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build()),
		});
		Dataset<Row> vertices = sqlCtx.read().option("delimiter", "\t").schema(vertices_schema).csv("src/main/resources/wiki-vertices.txt");
		vertices.show();
		// Page rank run
		GraphFrame gf = GraphFrame.apply(vertices, edges);
		PageRank pRank = gf.pageRank().resetProbability(0.01).maxIter(5);
		pRank.run().vertices().select("id", "pagerank").show();
	}
}
