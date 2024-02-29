const { SQSClient, DeleteMessageCommand, ReceiveMessageCommand} = require('@aws-sdk/client-sqs');

class SQSPoller {
    queueUrl;
    region;
    client;
    rabbitMQPublisher;
    pollIntervalMs;
    accessKeyId;
    accessKey;

    constructor(accessKeyId, accessKey, queueUrl, region, rabbitMQPublisher, pollIntervalMs = 1000) {
        this.accessKeyId = accessKeyId;
        this.accessKey = accessKey;
        this.queueUrl = queueUrl;
        this.region = region;
        this.pollIntervalMs = pollIntervalMs;
        this.rabbitMQPublisher = rabbitMQPublisher;

        this.client = new SQSClient({
            credentials: {
                accessKeyId: this.accessKeyId,
                secretAccessKey: this.accessKey
            },
            region: this.region,
            endpoint: this.queueUrl,
        });
    }

    async pollForMessages() {
        try {
            while (true) {
                const receiveParams = {
                    QueueUrl: this.queueUrl,
                    MaxNumberOfMessages: 10, // Adjust as needed
                    WaitTimeSeconds: 20 // Adjust as needed
                };

                const command = new ReceiveMessageCommand(receiveParams);
                const data = await this.client.send(command);

                if (data.Messages && data.Messages.length > 0) {
                    for (const message of data.Messages) {

                        // Process the message here...
                        await this.rabbitMQPublisher.publishMessage(message.Body, 'events')
                            .then(r => {
                                // Delete the message from the queue
                                this.deleteMessage(message.ReceiptHandle);
                            })
                            .catch(error => {
                                console.error('SQS ::: Error occurred while publishing to RabbitMQ', error);
                            });
                    }
                } else {
                    console.log('SQS ::: No messages available.');
                }

                await new Promise(resolve => setTimeout(resolve, this.pollIntervalMs));
            }
        } catch (error) {
            console.error('SQS ::: Error occurred while polling for messages:', error);
        }
    }

    async deleteMessage(receiptHandle) {
        try {
            const deleteParams = {
                QueueUrl: this.queueUrl,
                ReceiptHandle: receiptHandle
            };
            const command = new DeleteMessageCommand(deleteParams);
            await this.client.send(command);
        } catch (error) {
            console.error('SQS ::: Error occurred while deleting message from the queue:', error);
        }
    }
}

module.exports = SQSPoller;
