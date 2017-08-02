
const app = require('./app');

// Router setup.
app.use('/', require('./routes/index'));
// app.use('/users', require('./routes/users'));

module.exports = app;