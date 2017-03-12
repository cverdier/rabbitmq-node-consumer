'use strict';

const config = require('config');
const logger = require('../util/logger');
const rabbitMq = require('./rabbitmq');
const Promise = require('bluebird');
const messages = require('../message');

class BookingConsumer {
  constructor() {
    this.queue = config.get("consumer").queue;
    rabbitMq.subscribeQueue(this.queue, this.handleMessage.bind(this));
    logger.info("Created Consumer for : ", this.queue);
  }

  handleMessage(rawMessage) {
    return Promise.resolve()
      .then(() => {
        try {
          const message = JSON.parse(rawMessage)
          switch (message.type) {
            case (messages.BOOKING_CREATE_EVENT):
              return logger.debug(`Received Booking created event : ${JSON.stringify(message)}`)
            case (messages.BOOKING_CANCEL_EVENT):
              return logger.debug(`Received Booking cancel event : ${JSON.stringify(message)}`)
            default:
              return logger.warn(`Unsupported message type : ${message.type}`)
          }
        } catch (error) {
          logger.error(`Failed to handle message : ${rawMessage}`)
        }
      })
  }
}

const bookingConsumer = new BookingConsumer();

module.exports = bookingConsumer;
