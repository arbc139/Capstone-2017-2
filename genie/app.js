
const bodyParser = require('body-parser');
const cookieParser = require('cookie-parser');
const express = require('express');
const logger = require('morgan');
const path = require('path');

const app = express();

// View engine setup.
// TODO(dykim): Remove these engines when we don't use any views.
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

// Parsers, Loggers setup.
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());

module.exports = app;
