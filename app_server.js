var bodyParser = require('body-parser');
var cors = require('cors')
var express = require('express');

var service_interface = require('./routes/service_interface');
var config = require('./config');

var app = express();
var PORT = process.env.PORT || config.port;
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
  extended: true
}));

app.use('/api', service_interface);

app.listen(PORT, function() {
  console.log('Server listening on port %d', this.address().port);
});
