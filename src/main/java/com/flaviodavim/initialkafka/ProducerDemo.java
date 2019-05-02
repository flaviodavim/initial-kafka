package com.flaviodavim.initialkafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        /* O funcionamento de um Produtor é separado em 4 etapas:
         * 1. Definir as propriedades do Produtor
         * 2. Criar o Produtor
         * 3. Criar um registro para ser enviado
         * 4. Fazer o envio
         */

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
         */
        ProducerRecord<String, String> registro = new ProducerRecord<String, String>("first_topic", "Hello World!");

        /* O último passo é enviar o dado
         * Isso é feito utilizando um método do próprio Produtor
         * O send é um método assíncrono, então se o utilizarmos a aplicação é encerrada sem necessariamente os dados serem enviados
         * Para resolver o provlema de assíncronismo, podemos utilizar:
         *      -> flush: espera o envio
         *      -> close: espera o envio e depois fecha o Produtor
         */
        produtor.send(registro);

        produtor.flush();
        produtor.close();

    }
}
