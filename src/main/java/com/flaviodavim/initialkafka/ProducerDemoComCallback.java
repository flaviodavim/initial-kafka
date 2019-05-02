package com.flaviodavim.initialkafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoComCallback {

    public static void main(String[] args) {

        /* O funcionamento de um Produtor é separado em 4 etapas:
         * 1. Definir as propriedades do Produtor
         * 2. Criar o Produtor
         * 3. Criar um registro para ser enviado
         * 4. Fazer o envio
         */

        final Logger logger = LoggerFactory.getLogger(ProducerDemoComCallback.class);

        /* O primeiro passo é definir as propriedades do Produtor que vão ser utilizadas para a criação do Produtor.
         * Para definí-las, utilizamos o objeto Properties do Java
         * Cada propriedade é definida passando uma chave, que identifica a propriedade, e o valor que a define.
         * Para definir as chaves, utilizamos as variáveis estáticas do ProducerConfig
         * https://kafka.apache.org/documentation/#producerconfigs
         */
        String bootstrapServers = "127.0.0.1:9092";

        Properties propriedades = new Properties();
        propriedades.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        propriedades.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propriedades.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // As variáveis key.serializer e value.serializer ajudam o Produtor a saber os tipos de dados que ele está enviando
        // Isso é importante para o Produtor saber como serializar os dados
        // O StringSerializer é um tipo pronto que identifica o dado como String


        /* Em seguida, o segundo passo é criar o Produtor
         * O produtor é criado utilizando as propriedades definidas na passo 1
         * Na criação do produtor definimos que os objetos de chave e valor, nesse caso os dois serão String
         */

        KafkaProducer<String, String> produtor = new KafkaProducer<String, String>(propriedades);

        /* O terceiro passo é criar um registro
         * O registro é basicamente a mensagem que o Produtor vai enviar
         * O registro precisa indicar o tópico para quem vai mandar e a mensagem. A chave é opcional
         * Podemos criar vários registros e mandá-los enquanto o Produtor esteja ativo
         */
        for(int i=0; i < 10; i++) {
            ProducerRecord<String, String> registro =
                    new ProducerRecord<String, String>("first_topic", "Hello World! (" + Integer.toString(i) + ") ");

            /* O último passo é enviar o dado
             * Isso é feito utilizando um método do próprio Produtor
             * O send é um método assíncrono, então se o utilizarmos a aplicação é encerrada sem necessariamente os dados serem enviados
             * Para resolver o provlema de assíncronismo, podemos utilizar:
             *      -> flush: espera o envio
             *      -> close: espera o envio e depois fecha o Produtor
             */
            produtor.send(registro, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Esse método é executado sempre que um registro é enviado com sucesso ou quando é lançada alguma excessão
                    if (e == null) {
                        // Se essa condição for verdadeira, não houve exceção e o registro foi enviado com sucesso
                        logger.info(
                            "Recebe um novo metadado. \n" +
                            "Tópico: " + recordMetadata.topic() + "\n" +
                            "Partição: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        logger.error("Erro durante a produção", e) ;
                    }
                }
            });
        }

        produtor.flush();
        produtor.close();

    }
}
