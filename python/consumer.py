from confluent_kafka import Consumer

KAFKA_TOPIC = "TP-TRANSACTIONS"

class Consumers:
    def basicConsumer(self):
        print("Starting basicConsumer...")

        # Crear propiedades o configuraciones de nuestro consumer
        settings = {}        

        settings["bootstrap.servers"] = "localhost:9092"
        settings["group.id"] = "GR-TRANSACTIONS"

        # auto.offset.reset sirve para indicarle al consumer que lea desde el inicio del topic
        # opcional, si no se coloca, el consumer leerá desde el último offset
        settings["auto.offset.reset"] = "earliest"

        # enable.auto.commit sirve para que el consumer guarde el offset automáticamente
        # opcional, si no se coloca, el consumer no guardará el offset automáticamente
        settings["enable.auto.commit"] = "true"

        # auto.commit.interval.ms indica cada cuánto tiempo se guardará el offset
        # opcional, si no se coloca, el consumer guadará el offset cada 5 segundos
        settings["auto.commit.interval.ms"] = "1000"

        # Creamos el consumer
        consumer = Consumer(settings)

        try:
            # Subscribirnos al topic
            consumer.subscribe([KAFKA_TOPIC])

            print("Polling messages from topic...")

            while True:
                # Obtenemos el mensaje cada segundo
                msg = consumer.poll(1)

                if msg is None: continue

                if msg.error():
                    print("Constumer error: {}".format(msg.error()))
                    continue

                # Imprimir el mensaje recibido con el key, valor y partición
                print("Received message: Key {} Value {} Partition: {}".format(msg.key(), msg.value(), msg.partition()))

        except Exception as e:
            print(f"An error ocurred: {e}")
        finally:
            consumer.close()