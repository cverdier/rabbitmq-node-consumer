'use strict';

const moment = require('moment');
const winston = require('winston');
const config = require('config');

const loggerConfig = {
  levels: {
    error: 0,
    warn: 1,
    info: 2,
    verbose: 3,
    debug: 4,
    silly: 5
  },
  colors: {
    silly: 'magenta',
    verbose: 'magenta',
    debug: 'blue',
    info: 'green',
    warn: 'yellow',
    error: 'red'
  },
  transports: {
    console: {
      level: config.get('logger.console.level'),
      colorize: 'none',
      timestamp: 'YYYY-MM-DD HH:mm:ss.SSS'
    }
  }
};

winston.addColors(loggerConfig.colors);

const prettyPrint = obj => JSON.stringify(obj);

const consoleOpt = {
  level: loggerConfig.transports.console.level,
  colorize: true,
  prettyPrint,
  timestamp() {
    return moment().format(loggerConfig.transports.console.timestamp);
  },
  handleExceptions: false
};

const errorConsoleOpt = {
  colorize: true,
  timestamp() {
    return moment().format(loggerConfig.transports.console.timestamp);
  },
  handleExceptions: true,
  humanReadableUnhandledException: true
};

const options = {
  levels: loggerConfig.levels,
  transports: [
    new (winston.transports.Console)(consoleOpt)
  ],
  exceptionHandlers: [
    new winston.transports.Console(errorConsoleOpt)
  ],
  exitOnError: false
};

const logger = new (winston.Logger)(options);

module.exports = logger;
