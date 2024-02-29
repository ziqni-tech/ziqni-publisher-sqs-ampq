const amqp = require('amqplib');

class RabbitMQPublisher {

    connectionUrl;
    exchangeName;
    username;
    password;
    channel;
    connection;
    log;

    constructor(connectionUrl, exchangeName, username, password, log) {
        this.connectionUrl = connectionUrl;
        this.exchangeName = exchangeName;
        this.username = username;
        this.password = password;
        this.log = log;
    }

    async init() {
        const opt = { credentials: require('amqplib').credentials.plain(this.username, this.password) };
        this.connection = await amqp.connect(this.connectionUrl, opt, (err, conn) => {
            this.log("RabbitMQ :::  [*] Failed to connect %s", err);
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
            console.error('RabbitMQ ::: Error occurred while closing the RabbitMQ publisher:', error);
        }
    }

    async publishMessage(message, routingKey) {
        try {
            // Publish the message to the exchange
            this.channel.publish(this.exchangeName, routingKey, Buffer.from(message));
            return true;
        } catch (error) {
            console.error('Error occurred while publishing message:', error);
            return false;
        }
    }
}

module.exports = RabbitMQPublisher;
