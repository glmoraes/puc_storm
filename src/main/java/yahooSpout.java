import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

import static java.lang.Thread.*;

public class yahooSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    //inicializa tudo o que é preciso e faz conexao com banco se for o caso
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    //utiliza o que foi inicializado - fica pedindo novos dados, novos dados
    public void nextTuple() {
        try {
            //obter dados da quotação do Google
            System.out.println("passei aqui3");
            StockQuote quote = YahooFinance.get("GOOG").getQuote();
            System.out.println("passei aqui4");
            //quotacao
            BigDecimal preco = quote.getPrice();
            //fechamento do dia anterior
            BigDecimal fechamentoPrevio = quote.getPreviousClose();
            //data da coleta
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            //evnvia as informações  (tupla) obtidas
            collector.emit(new Values ("GOOG", sdf.format(timestamp),preco.doubleValue(),fechamentoPrevio.doubleValue()));
            //fica dormindo para pegar informação novamente
            Thread.sleep(1000);
        }
        catch (Exception e) {}
    }
    //obrigatório nos spout e bouts (mesmo que esteja  vazio) - open e nextTuple tb
    //mapeia o conteudo do emit - campos de saída da topologia
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("empresa","timestamp","preco","fechamentoPrevio"));
    }
}
