'use strict';

class Envelope {
  constructor(type, content, origin, version) {
    this.type = type;
    this.content = content;
    this.origin = origin;
    this.eventDate = new Date();
    this.version = version || 1;
  }
}

module.exports = {
  BOOKING_CREATE_EVENT: "BOOKING_CREATE_EVENT",
  BOOKING_CANCEL_EVENT: "BOOKING_CANCEL_EVENT",

  createEnvelope: (type, content, version) => new Envelope(type, content, version)
};
