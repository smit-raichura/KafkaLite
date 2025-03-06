from .server.kafka_server import KafkaServer

if __name__ == "__main__":
    server = KafkaServer("localhost", 9092)
    server.start()
    

