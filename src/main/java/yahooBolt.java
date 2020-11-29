import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.Map;

public class yahooBolt extends BaseBasicBolt {
    //semelhante ao open do bolt - fazer alguma coisa necessária
    public void prepare(Map stormConf, TopologyContext context) {}
    //pega a tupla gerada pelo spout e fazer algum processamento
    public void execute(Tuple input, BasicOutputCollector collector) {
        //pega a posição 0 da tupla (empresa)
        String empresa = input.getValue(0).toString();
        String timestamp = input.getString(1);
        Double preco = (Double) input.getValueByField("preco");
        Double fechamentoAnterior = input.getDoubleByField("fechamentoPrevio");
        Boolean ganho = true;

        if (preco<=fechamentoAnterior) {
            ganho = false;
            System.out.println(("Ocorreu perda ou manteve o valor da ação."));
        }
        else
            System.out.println("Ocorreu ganho.");
        //no lugar desses prints poderia ser uma notificação/email para o usuário
        System.out.println(empresa);
        System.out.println(timestamp);
        System.out.println(preco);
        System.out.println(fechamentoAnterior);
        System.out.println(ganho);

        collector.emit(new Values(empresa, timestamp, preco, ganho));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("empresa","timestamp","preco","ganho"));
    }
    //finalização do que não é mais necessário
    public void cleanup () {}
}
