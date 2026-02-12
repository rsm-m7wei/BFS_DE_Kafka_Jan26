from confluent_kafka.admin import AdminClient,NewTopic,ConfigResource

class cdcClient(AdminClient):
    '''
    AdminClient that deals with the Kafka topic partitions etc.
    
    '''
    def __init__(self):
        config = {'bootstrap.servers': 'localhost:29092'}
        super().__init__(config)

    def topic_exists(self, topic):
        metadata = self.list_topics()
        for t in iter(metadata.topics.values()):
            if t.topic == topic:
                return True
        return False

    def create_topic(self, topic,num_partitions):
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)  #only 1 broker in yml
        result_dict = self.create_topics([new_topic])
        for topic, future in result_dict.items():
            try:
                future.result()  # The result itself is None
                print("Topic {} created with {} partitions".format(topic,num_partitions))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))

    def get_consumer_group_size(self,group_id):
    # Fetch consumer group details
        group_metadata = self.list_groups(group=group_id)
        # Extract and return the number of members (consumers) in the group
        print(group_metadata)
        for group in group_metadata:
            if group.id == group_id:
                return len(group.members)
        return 0


    def delete_topic(self,topics):
        fs = self.delete_topics(topics, operation_timeout=5)
        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))

if __name__ == '__main__':
    client = cdcClient()
    employee_topic_name = "bf_employee_cdc"
    num_parition = 3
    if client.topic_exists(employee_topic_name):
        client.delete_topic([employee_topic_name])
    else:
        client.create_topic(employee_topic_name, num_parition)
    