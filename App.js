const config = require('./config.json');
const winston = require('winston');
const http = require('http');
const fs = require('fs');

const log = function (entry) {
    fs.appendFileSync('/tmp/ziqni-webhook-app.log', new Date().toISOString() + ' - ' + entry + '\n');
};

const logDirectory = './tmp';
if (!fs.existsSync(logDirectory)) {
    fs.mkdirSync(logDirectory);
}

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
      winston.format.timestamp({
          format: 'YYYY-MM-DD HH:mm:ss'
      }),
      winston.format.errors({ stack: true }),
      winston.format.splat(),
      winston.format.json()
    ),
    defaultMeta: { service: 'ziqni-webhook-app' },
    transports: [
        new winston.transports.File({ filename: './tmp/ziqni-webhook-app.log' })
    ]
});

const RabbitMQPublisher = require('./RabbitMQPublisher');
const publisher = new RabbitMQPublisher(config.rabbitConnectionUrl, config.rabbitExchange, config.rabbitUser, config.rabbitPass, logger);

const SQSPoller = require('./SQSPoller');
const poller = new SQSPoller(config.sqsAccessKeyId, config.sqsAccessKey, config.sqsQueueUrl, config.sqsRegion, publisher, logger);

const port = process.env.PORT || 3000,
    html = fs.readFileSync('index.html');

const server = http.createServer(async function (req, res) {
    if (req.url === '/stats') {
        const stats = {
            sqs: {
                queue: config.sqsQueueUrl,
                status: poller.isPolling ? 'Connected' : 'Disconnected',
                startedAt: poller.startedAt ? poller.startedAt.toString() : '',
                lastMessageReceived: poller.lastMessageReceived ? poller.lastMessageReceived.toString() : '',
                messagesProcessed: poller.messagesProcessed
            },
            rabbit: {
                status: publisher.isConnected() ? 'Connected' : 'Disconnected',
                startedAt: publisher.startedAt ? publisher.startedAt.toString() : '',
                messagesProcessed: publisher.messagesProcessed,
                exchange: config.rabbitExchange,
                lastMessageSent: publisher.lastMessageSent ? publisher.lastMessageSent.toString() : '',
                routingKey: 'events' // Assuming fixed routing key for all messages
            },
        };
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(stats));
    } else {
        res.writeHead(200);
        res.write(html);
        res.end();
    }
});

// Listen on port 3000, IP defaults to 127.0.0.1
server.listen(port);

// Put a friendly message on the terminal
console.log('HTTP ::: Server running at http://127.0.0.1:' + port + '/');
logger.info('HTTP ::: Server running at http://127.0.0.1:' + port + '/');

// Start the RabbitMQ ::: publisher and SQS poller
logger.info('RabbitMQ ::: Start initializing RabbitMQ ::: publisher...');
publisher.init()
    .then(data => {
        logger.info('RabbitMQ ::: Publisher initialized.');
        beginPolling();
    })
    .catch(error => {
        logger.error('RabbitMQ ::: Error occurred while initializing RabbitMQ ::: publisher:', error);
    });

function beginPolling(){
    // Start polling for messages from SQS and publishing to RabbitMQ
    logger.info('SQS ::: Start polling for messages from SQS ' + config.sqsQueueUrl + ' and publishing to RabbitMQ ::: exchange ' + config.rabbitExchange + '...');
    poller.pollForMessages().then(r => {
        publisher.close().then(r => logger.info('RabbitMQ ::: publisher closed.') );
        logger.info('SQS ::: polling for messages completed.');
    });
}

