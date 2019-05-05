package com.flaviodavim.initialkafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoComThreads {

    public static void main(String[] args) {
        new ConsumerDemoComThreads().run();
    }

    private ConsumerDemoComThreads() {}

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoComThreads.class.getName());

        String servidorBootstrap = "127.0.0.1:9092";
        String idGrupo = "my-sixth-application";
        String topico = "first_topic";

        // O latch serve para lidar com múltiplas threads
        CountDownLatch latch = new CountDownLatch(1);

        // Estamos criando um novo objeto ConsumerRunnable
        logger.info("Criando a thread consumidora.");
        Runnable myConsumerRunnable = new ConsumerRunnable(topico, idGrupo, servidorBootstrap, latch);

        // Aqui estamos inicializando a thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Adicionamos um gancho de desligamento
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Pego pelo gancho de desligamento.");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();

           try {
               latch.await();
           } catch (InterruptedException e) {
               e.printStackTrace();
           }

           logger.info("Aplicação foi finalizada.");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Aplicação interrompida.", e);
        } finally {
            logger.info("Aplicação fechando.");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumidor;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String topico,
                              String idGrupo,
                              String servidorBootstrap,
                              CountDownLatch latch) {
            this.latch = latch;

            /* O primeiro passo é definir as propriedades do Consumidor que vão ser utilizadas para a sua criação
             * Para definí-las, utilizamos o objeto Properties do Java
             * Cada propriedade é definida passando uma chave, que identifica a propriedade, e o valor que a define.
             * Para definir as chaves, utilizamos as variáveis estáticas do ConsumerConfig
             * https://kafka.apache.org/documentation/#consumerconfigs
             */
            Properties propriedades = new Properties();
            propriedades.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidorBootstrap);
            propriedades.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            propriedades.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            // As variáveis key.serializer e value.serializer ajudam o Consumidor a saber os tipos de dados que ele está recebendo
            // Isso é importante para o Consumidor saber como deserializar os dados
            // O StringSerializer é um tipo pronto que identifica o dado como String
            propriedades.setProperty(ConsumerConfig.GROUP_ID_CONFIG, idGrupo);
            propriedades.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // A variável acima indica a quais dados devem ser lidos. A opções são:
            //      -> earliest: deseja ler desde o início do tópico
            //      -> latest: deseja ler apenas as mensagens que estão chegando
            //      -> none: lança um erro

            /* Em seguida, vamos criar o Consumidor
             * O Consumidor é criado utilizando as propriedades definidas na passo acima
             * Na criação do Consumidor definimos que os objetos de chave e valor, nesse caso os dois serão String
             */
            consumidor = new KafkaConsumer<String, String>(propriedades);
            /* Depois vamos inscrever o Consumidor ao tópico */
            consumidor.subscribe(Collections.singleton(topico));

        }

        @Override
        public void run() {
            /* Aqui é feito o recebimento dos dados
             * O dados não são consultados enquanto não forem solicitados
             * O método poll serve para captar os dados dos tópicos
             * O poll recebe o objeto Duration que indica o tempo que o Consumidor tem para terminar de consumir os dados
             * O poll vai consultar todos os registros desse período
             * A forma de consulta é consultar todos os dados de uma partição antes de seguir para a próxima
             */
            try {
                while(true) {
                    ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> registro : registros) {
                        // O poll vai trazer os registros que foram consultados a cada tempo, e exibimos cada mensagem lida
                        logger.info(
                                "Key: " + registro.key() + ", Value: " + registro.value() + "\n" +
                                        "Partition: " + registro.partition() + ", Offset: " + registro.offset()
                        );
                    }
                }
            } catch (WakeupException e) {
                logger.info("Recebeu sinal de encerramento.");
            } finally {
                consumidor.close();
                latch.countDown(); // Avisa ao código principal que esse Consumidor foi finalizado
            }
        }

        public void shutDown() {
            // Esse método é um método especial para interromper o consumidor.poll()
            // Ele lança uma exceção: WakeUpException
            consumidor.wakeup();
        }
    }
}
