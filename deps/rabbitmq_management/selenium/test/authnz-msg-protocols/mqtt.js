const assert = require('assert')
const { getURLForProtocol } = require('../utils')
const { reset, expectUser, expectVhost, expectResource, allow, verifyAll } = require('../mock_http_backend')
const mqtt    = require('mqtt');

const profiles = process.env.PROFILES || ""
var backends = ""
for (const element of profiles.split(" ")) {
  if ( element.startsWith("auth_backends-") ) {
    backends = element.substring(element.indexOf("-")+1)
  }
}

describe('Having MQTT protocol enbled and the following auth_backends: ' + backends, function () {
  let mqttOptions
  let expectations = []
  let client_id = 'selenium-client'
  let rabbit = process.env.RABBITMQ_HOSTNAME || 'localhost'

  before(function () {
    mqttOptions = {
      clientId: client_id,
      protocolId: 'MQTT',
      protocolVersion: 4,
      keepalive: 10000,
      clean: false,
      reconnectPeriod: '1000',
      username: 'mqtt_u',
      password: 'mqtt_p',
    }
    if ( backends.includes("http") ) {
      reset()
      expectations.push(expectUser({ "username": "mqtt_u", "password": "mqtt_p", "client_id": client_id, "vhost": "/" }, "allow"))
      expectations.push(expectVhost({ "username": "mqtt_u", "vhost": "/"}, "allow"))
      expectations.push(expectResource({ "username": "mqtt_u", "vhost": "/", "resource": "queue", "name": "mqtt-will-selenium-client", "permission":"configure", "tags":"", "client_id" : client_id }, "allow"))
    }
  })

  it('can open an MQTT connection', function () {
    var client = mqtt.connect("mqtt://" + rabbit + ":1883", mqttOptions)
    client.on('error', function(err) {
      assert.fail("Mqtt connection failed due to " + err)
      client.end()
    })
    client.on('connect', function(err) {
      client.end()
    })    
  })

  after(function () {
      if ( backends.includes("http") ) {
        verifyAll(expectations)
      }
  })
})