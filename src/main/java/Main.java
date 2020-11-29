import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Main {
    public static void main(String[] args)  throws InterruptedException, Exception {
        //constroi a topologia
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("yahooSpout",new yahooSpout());
        // 3 bolts em paralelo
        builder.setBolt("yahooBolt", new yahooBolt(),3).shuffleGrouping("yahooSpout");
        //é possivel usar uma configuraçã de um arquivo ou diretorio
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try {
            System.out.println("passei aqui1");
            cluster.submitTopology("TopologiaYahoo",conf,builder.createTopology());
            //sleep para teste
            System.out.println("passei aqui2");
            Thread.sleep(50000);
            cluster.shutdown();
        }
        catch (Exception e){
        }
    }
}
