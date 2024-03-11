const amqp = require('amqplib');

class RabbitMQPublisher {

    connectionUrl;
    exchangeName;
    username;
    password;
    channel;
    connection;
    logger;

    constructor(connectionUrl, exchangeName, username, password, logger) {
        this.connectionUrl = connectionUrl;
        this.exchangeName = exchangeName;
        this.username = username;
        this.password = password;
        this.logger = logger;

        this.startedAt = new Date();
        this.lastMessageSent = null;

        this.messagesProcessed = 0;
        this.messageBuffer = [];
    }

    isConnected() {
        return this.connection && !this.connection.closed;
    }

    async init() {
        const opt = { credentials: require('amqplib').credentials.plain(this.username, this.password) };
        this.connection = await amqp.connect(this.connectionUrl, opt, (err, conn) => {
            this.logger.error("RabbitMQ :::  [*] Failed to connect %s", err);
        });

        this.channel = await this.connection.createChannel();

        // make sure the exchange is created
        await this.channel.assertExchange(this.exchangeName, "direct", {
            durable: true, xQueueMode: "default"
        });
    }

    async close() {
        try {
            // Close the channel and the connection
            await this.channel.close();
            await this.connection.close();
        } catch (error) {
            this.logger.error('RabbitMQ ::: Error occurred while closing the RabbitMQ publisher:', error);
        }
    }

    async publishMessage(message, routingKey) {
        try {
            // If there is an active connection, we send the message immediately
            if (this.isConnected()) {
                // Publish the message to the exchange
                this.channel.publish(this.exchangeName, routingKey, Buffer.from(message));
                this.lastMessageSent = new Date();
                this.messagesProcessed++;
            } else {
                // If there is no active connection, add the message to the buffer
                this.messageBuffer.push({ message, routingKey });
            }
            return true;
        } catch (error) {
            this.logger.error('Error occurred while publishing message:', error);
            await this.reconnectToRabbitMQ();
            return false;
        }
    }

    async reconnectToRabbitMQ() {
        try {
            // Close existing channel and connection
            await this.close();

            // Recreate connection and channel with RabbitMQ
            const opt = { credentials: require('amqplib').credentials.plain(this.username, this.password) };
            this.connection = await amqp.connect(this.connectionUrl, opt);
            this.channel = await this.connection.createChannel();
            await this.channel.assertExchange(this.exchangeName, 'direct', { durable: true, xQueueMode: 'default' });
            this.logger.info('RabbitMQ ::: Successfully reconnected to RabbitMQ.');

            // Sending messages from the buffer after a successful reconnection
            for (const { message, routingKey } of this.messageBuffer) {
                await this.channel.publish(this.exchangeName, routingKey, Buffer.from(message));
                this.lastMessageSent = new Date();
                this.messagesProcessed++;
            }
            // Clear the buffer after sending all messages
            this.messageBuffer = [];
        } catch (error) {
            this.logger.error('RabbitMQ ::: Error occurred while reconnecting to RabbitMQ:', error);
        }
    }
}

module.exports = RabbitMQPublisher;
