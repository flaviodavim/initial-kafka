package com.flaviodavim.initialkafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {


        /* O funcionamento de um Consumidor é separado em 4 etapas:
         * 1. Definir as propriedades do Consumidor
         * 2. Criar o Consumidor
         * 3. Inscrever o Consumidor para nosso tópico
         * 4. Consulta o dado
         */
        final Logger logger = LoggerFactory.getLogger(ProducerDemoComChaves.class);

        /* O primeiro passo é definir as propriedades do Consumidor que vão ser utilizadas para a sua criação
         * Para definí-las, utilizamos o objeto Properties do Java
         * Cada propriedade é definida passando uma chave, que identifica a propriedade, e o valor que a define.
         * Para definir as chaves, utilizamos as variáveis estáticas do ConsumerConfig
         * https://kafka.apache.org/documentation/#consumerconfigs
         */
        String bootstrapServers = "127.0.0.1:9092";
        String idGrupo = "my-fourth-application";
        String topico = "first_topic";

        Properties propriedades = new Properties();
        propriedades.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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


        /* Em seguida, o segundo passo é criar o Consumidor
         * O Consumidor é criado utilizando as propriedades definidas na passo 1
         * Na criação do Consumidor definimos que os objetos de chave e valor, nesse caso os dois serão String
         */

        KafkaConsumer<String, String> consumidor = new KafkaConsumer<String, String>(propriedades);

        /* O terceiro passo é inscrever o Consumidor ao tópico
         */
        consumidor.subscribe(Collections.singleton(topico));
        // consumidor.subscribe(Arrays.asList("first_topic", "second_topic"));

        /* O último passo é receber o dado
         * O dados não são consultados enquanto não forem solicitados
         * O método poll serve para captar os dados dos tópicos
         * O poll recebe o objeto Duration que indica o tempo que o Consumidor tem para terminar de consumir os dados
         * O poll vai consultar todos os registros desse período
         * A forma de consulta é consultar todos os dados de uma partição antes de seguir para a próxima
         */

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

    }
}
