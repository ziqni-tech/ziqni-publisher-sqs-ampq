const { SQSClient, DeleteMessageCommand, ReceiveMessageCommand} = require('@aws-sdk/client-sqs');

class SQSPoller {
    queueUrl;
    region;
    client;
    rabbitMQPublisher;
    pollIntervalMs;
    accessKeyId;
    accessKey;

    constructor(accessKeyId, accessKey, queueUrl, region, rabbitMQPublisher, logger, pollIntervalMs = 1000) {
        this.accessKeyId = accessKeyId;
        this.accessKey = accessKey;
        this.queueUrl = queueUrl;
        this.region = region;
        this.logger = logger;
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

        // Add variables to track time
        this.startedAt = new Date();
        this.lastMessageReceived = null;

        this.messagesProcessed = 0;

        this.messageBuffer = [];

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
                    const command = new ReceiveMessageCommand(receiveParams);
                    const data = await this.client.send(command);

                    if (data.Messages && data.Messages.length > 0) {
                        for (const message of data.Messages) {
                            this.lastMessageReceived = new Date();
                            // Process the message here...
                            if (this.rabbitMQPublisher.isConnected()) {
                                await this.rabbitMQPublisher.publishMessage(message.Body, 'events')
                                  .then(r => {
                                      this.deleteMessage(message.ReceiptHandle);
                                      this.messagesProcessed++;
                                  })
                                  .catch(error => {
                                      this.logger.error('SQS ::: Error occurred while publishing to RabbitMQ', error);
                                  });
                            } else {
                                // If there is no active connection, add the message to the buffer
                                this.messageBuffer.push(message);
                            }
                        }
                    } else {
                        this.logger.info('SQS ::: No messages available.');
                    }
                } catch (error) {
                    this.logger.error('SQS ::: Error occurred while receiving messages:', error);

                    // Handle disconnection or errors and attempt reconnection
                    await this.reconnect();
                }

                await new Promise(resolve => setTimeout(resolve, this.pollIntervalMs));
            }
        } catch (error) {
            this.logger.error('SQS ::: Error occurred while polling for messages:', error);
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
            this.logger.error('SQS ::: Error occurred while deleting message from the queue:', error);
        }
    }

    async reconnect() {
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

            // Sending buffered messages after successful reconnection
            if (this.messageBuffer.length > 0) {
                this.logger.info(`SQS ::: Sending ${this.messageBuffer.length} messages from buffer after reconnection.`);
                for (const message of this.messageBuffer) {
                    await this.rabbitMQPublisher.publishMessage(message.Body, 'events')
                      .then(r => {
                          this.deleteMessage(message.ReceiptHandle);
                          this.messagesProcessed++;
                      })
                      .catch(error => {
                          console.error('SQS ::: Error occurred while publishing to RabbitMQ', error);
                      });
                }
                // Clearing the buffer after sending messages
                this.messageBuffer = [];
            }
            this.logger.info('SQS ::: Reconnected successfully.');
        } catch (error) {
            this.logger.error('SQS ::: Error occurred while reconnecting:', error);
        }
    }
}

module.exports = SQSPoller;
