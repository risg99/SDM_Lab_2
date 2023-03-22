package exercise_3;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import exercise_2.Exercise_2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import org.apache.spark.sql.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VertexProperty implements Serializable {
        private Integer cost;
        private List<Long> path;

        public VertexProperty(Integer cost, List<Long> path) {
            this.cost = cost;
            this.path = path;
        }

        public Integer getCost() {
            return cost;
        }

        public void setCost(Integer cost) {
            this.cost = cost;
        }

        public List<Long> getPath() {
            return path;
        }

        public void setPath(List<Long> path) {
            this.path = path;
        }
    }

    private static class VProg extends AbstractFunction3<Long, VertexProperty, VertexProperty, VertexProperty> implements Serializable {
        @Override
        public VertexProperty apply(Long vertexID, VertexProperty vertexValue, VertexProperty message){
            System.out.println(message.getCost());
//            return vertexValue;

            if (message.getCost() >= Integer.MAX_VALUE || message.getCost()<0) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
                System.out.println("I am here");
                Integer newCost = Math.min(vertexValue.getCost(),message.getCost());
                List<Long> newPath;
//                Sysprint(message.getPath());
                if (newCost.equals(vertexValue.getCost())) {
                    System.out.println(vertexValue.getPath());
                    newPath = message.getPath();
                } else {
                    newPath = message.getPath();
                }
                return new VertexProperty(newCost, newPath);
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<VertexProperty,Integer>, Iterator<Tuple2<Object,VertexProperty>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, VertexProperty>> apply(EdgeTriplet<VertexProperty, Integer> triplet) {
            Tuple2<Object,VertexProperty> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,VertexProperty> dstVertex = triplet.toTuple()._2();
            Integer weight = triplet.toTuple()._3();
            if (sourceVertex._2.getCost() + weight >= dstVertex._2.getCost() || sourceVertex._2.getCost() + weight < 0) {   // source vertex value is smaller than dst vertex?
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,VertexProperty>>().iterator()).asScala();
            } else {
                // propagate source vertex value and path
                List<Long> newPath = new ArrayList<>(sourceVertex._2.getPath());
                newPath.add((Long) triplet.dstId());
                System.out.println(newPath);
                VertexProperty newVertexValue = new VertexProperty(sourceVertex._2.getCost() + weight, newPath);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,VertexProperty>(triplet.dstId(), newVertexValue)).iterator()).asScala();
            }
        }
    }
    private static class mergeMsg extends AbstractFunction2<VertexProperty, Tuple2<Object, VertexProperty>, VertexProperty> implements Serializable {
        @Override
        public VertexProperty apply(VertexProperty v1, Tuple2<Object, VertexProperty> v2) {
            return v1.getCost() <= v2._2.getCost() ? v1 : v2._2;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {

        ArrayList<Long> arr = new ArrayList<Long>();
        arr.add(1L);
        List<Tuple2<Object, VertexProperty>> verticesList = Lists.newArrayList(
                new Tuple2<>(1L, new VertexProperty(0,arr)),
                new Tuple2<>(2L, new VertexProperty(Integer.MAX_VALUE, new ArrayList<>())),
                new Tuple2<>(3L, new VertexProperty(Integer.MAX_VALUE, new ArrayList<>())),
                new Tuple2<>(4L, new VertexProperty(Integer.MAX_VALUE, new ArrayList<>())),
                new Tuple2<>(5L, new VertexProperty(Integer.MAX_VALUE, new ArrayList<>())),
                new Tuple2<>(6L, new VertexProperty(Integer.MAX_VALUE, new ArrayList<>()))
        );
        JavaRDD<Tuple2<Object, VertexProperty>> vertices = ctx.parallelize(verticesList);

        // edges RDD
        List<Edge<Integer>> edgesList = Arrays.asList(
//                new Edge<>(1L, 2L, 1),
//                new Edge<>(2L, 3L, 1),
//                new Edge<>(3L, 4L, 1),
//                new Edge<>(4L, 5L, 1),
//                new Edge<>(5L, 1L, -5)
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );
        JavaRDD<Edge<Integer>> edges = ctx.parallelize(edgesList);

        int source = 1;


        int numIter = 5;
        Graph<VertexProperty, Integer> resultGraph = Graph.apply(vertices.rdd(),edges.rdd(), new VertexProperty(Integer.MAX_VALUE, new ArrayList<>()),StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), ClassTag$.MODULE$.apply(VertexProperty.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(resultGraph, ClassTag$.MODULE$.apply(VertexProperty.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new VertexProperty(Integer.MAX_VALUE, new ArrayList<>()),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new Exercise_3.VProg(),
                new Exercise_3.sendMsg(),
                new Exercise_3.mergeMsg(),
                ClassTag$.MODULE$.apply(VertexProperty.class)).vertices().toJavaRDD()
                .foreach(v -> {
                    Tuple2<Object,VertexProperty> vertex = (Tuple2<Object, VertexProperty>)v;
                    System.out.println("Minimum cost to get from  to "+vertex._1+" is "+vertex._2.getCost()+vertex._2.getPath());
                });
//        resultDf.show();

    }
}
