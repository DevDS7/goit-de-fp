from kafka.admin import KafkaAdminClient, NewTopic
from kafka_configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

num_partitions = 2
replication_factor = 1

input_topic = "athlete_event_results_ds"
output_topic = "athlete_aggregated_results_ds"

new_topics = [
    NewTopic(
        name=input_topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    ),
    NewTopic(
        name=output_topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )
]

try:
    admin_client.create_topics(
        new_topics=new_topics,
        validate_only=False
    )
    print("Topics created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

print(admin_client.list_topics())

admin_client.close()