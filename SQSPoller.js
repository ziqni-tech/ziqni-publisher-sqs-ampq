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

        // Flag to control the execution of the polling cycle
        this.isPolling = true;
    }



    async pollForMessages() {
        try {
            const receiveParams = {
                QueueUrl: this.queueUrl,
                MaxNumberOfMessages: 10, // Adjust as needed
                WaitTimeSeconds: 20 // Adjust as needed
            };

            while (this.isPolling) {
                try {
                    // Creating an intentional error to check reconnection
                    // if (Math.random() < 0.5) { // Example: 50% probability
                    //     throw new Error('Intentional error occurred');
                    // }

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
                } catch (error) {
                    console.error('SQS ::: Error occurred while receiving messages:', error);

                    // Handle disconnection or errors and attempt reconnection
                    await this.reconnect();
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

    async reconnect() {
        console.log('SQS ::: Reconnecting...');
        try {
            // Reinitialize SQS client
            this.client = new SQSClient({
                credentials: {
                    accessKeyId: this.accessKeyId,
                    secretAccessKey: this.accessKey
                },
                region: this.region,
                endpoint: this.queueUrl,
            });
            console.log('SQS ::: Reconnected successfully.');
        } catch (error) {
            console.error('SQS ::: Error occurred while reconnecting:', error);
        }
    }
}

module.exports = SQSPoller;
