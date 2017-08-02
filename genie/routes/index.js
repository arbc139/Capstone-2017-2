const express = require('express');
const router = express.Router();

router.get('/', function(req, res, next) {
  // console.log('req query: ', req.query);
  res.render('index', { title: 'Express' });
});

router.post('/', function(req, res) {
  // console.log('req body: ', req.body);
});

module.exports = router;
