'use strict';

const amqplib = require('amqplib');
const Promise = require('bluebird');
const config = require('config');
const _ = require('lodash');
const logger = require('../util/logger');

class RabbitMQ {

  constructor() {
    this.rabbitConfig = config.get('rabbitmq');
    const rabbitMqUrl = `amqp://${this.rabbitConfig.user}:${this.rabbitConfig.password}@${this.rabbitConfig.host}`;
    logger.info('Starting RabbitMQ connection :', rabbitMqUrl);
    this.handler = amqplib.connect(rabbitMqUrl, { keepAlive: true });
  }

  sendMessage(exchange, key, message) {
    return this.handler
      .then(connection => connection.createChannel())
      .then(channel => channel.assertExchange(exchange, 'topic', { durable: true })
        .then(() => channel.publish(exchange, key, new Buffer(JSON.stringify(message))))
        .then(() => logger.debug('Sent message :', message))
        .then(() => channel.close())
      )
      .catch(logger.error);
  }

  subscribeQueue(queue, messageHandler) {
    return this.handler
      .then(conn => conn.createChannel())
      .then(channel => {
        logger.info(`Consuming from ${queue}`);
        return channel.assertQueue(queue, { exclusive: false })
          .then(() => channel.consume(queue, message => messageHandler(message.content.toString()), { noAck: true }))
          .then(() => logger.info(`Consumed from ${queue}`))
      })
      .catch(logger.error);
  }
}

const rabbitMq = new RabbitMQ();

module.exports = rabbitMq;
