const config = require('./config.json');

const log = function (entry) {
    fs.appendFileSync('/tmp/ziqni-webhook-app.log', new Date().toISOString() + ' - ' + entry + '\n');
};

const RabbitMQPublisher = require('./RabbitMQPublisher');
const publisher = new RabbitMQPublisher(config.rabbitConnectionUrl, config.rabbitExchange, config.rabbitUser, config.rabbitPass, log);

const SQSPoller = require('./SQSPoller');
const poller = new SQSPoller(config.sqsAccessKeyId, config.sqsAccessKey, config.sqsQueueUrl, config.sqsRegion, publisher);

const port = process.env.PORT || 3000,
    http = require('http'),
    fs = require('fs'),
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

// Start the RabbitMQ ::: publisher and SQS poller
console.log('RabbitMQ ::: Start initializing RabbitMQ ::: publisher...');
publisher.init()
    .then(data => {
        console.log('RabbitMQ ::: Publisher initialized.');
        beginPolling();
    })
    .catch(error => {
        console.error('RabbitMQ ::: Error occurred while initializing RabbitMQ ::: publisher:', error);
    });

function beginPolling(){
    // Start polling for messages from SQS and publishing to RabbitMQ
    console.log('SQS ::: Start polling for messages from SQS ' + config.sqsQueueUrl + ' and publishing to RabbitMQ ::: exchange ' + config.rabbitExchange + '...');
    poller.pollForMessages().then(r => {
        publisher.close().then(r => console.log('RabbitMQ ::: publisher closed.') );
        console.log('SQS ::: polling for messages completed.');
    });
}

