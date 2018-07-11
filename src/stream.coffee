url = require 'url'
events = require 'events'
request = require 'request'
JSONStream = require './json_stream'

baseURL = ->
  url.parse(process.env.FLOWDOCK_STREAM_URL || 'https://stream.flowdock.com/flows')

backoff = (backoff, errors, operator = '*') ->
  console.log('node-flowdock: steam->backoff', backoff, errors, oeprator)
  Math.min(
    backoff.max,
    (if operator == '+'
      errors
    else
      Math.pow 2, errors - 1) * backoff.delay
  )

class Stream extends events.EventEmitter
  constructor: (@auth, @flows, @params = {}) ->
    @networkErrors = 0
    @responseErrors = 0
    @on 'reconnecting', (timeout) =>
      console.log('node-flowdock: steam->constructor->on.reconnecting', timeout)
      setTimeout =>
        @connect()
      , timeout

  # Start new connection to Flowdock API
  #
  # Returns request object
  connect: ->
    return if @disconnecting

    errorHandler = (error) =>
      @networkErrors += 1
      console.log('node-flowdock: steam->connect->errorHandler', error)
      @emit 'clientError', 0, 'Network error'
      @emit 'reconnecting', (backoff Stream.backoff.network, @networkErrors, '+')

    @request = request(@options()).on 'response', (response) =>
      @request.removeListener 'error', errorHandler

      @networkErrors = 0
      if response.statusCode >= 400
        @responseErrors += 1
        console.log('node-flowdock: steam->connect->request->statusCode > 400', response)
        @emit 'clientError', response.statusCode
        @emit 'reconnecting', (backoff Stream.backoff.error, @responseErrors, '*')
      else
        @responseErrors = 0
        parser = new JSONStream()
        parser.on 'data', (message) =>
          @emit 'message', message

        @request.on 'abort', =>
          console.log('node-flowdock: steam->connect->request->abort')
          parser.removeAllListeners()
          @emit 'disconnected'
          @emit 'end'

        parser.on 'end', =>
          console.log('node-flowdock: steam->connect->request->end')
          parser.removeAllListeners()
          @emit 'disconnected'
          @emit 'clientError', 0, 'Disconnected'
          @emit 'reconnecting', 0

        console.log('node-flowdock: steam->connect->connected')
        @request.pipe parser
        @emit 'connected'
    @request.once 'error', errorHandler
    @request

  # Generate request options
  options: ->
    options =
      uri: baseURL()
      qs: filter: @flows.join ','
      method: 'GET'
      headers:
        'Authorization': @auth
        'Accept': 'application/json'

    for key, value of @params
      options.qs[key] = value
    options

  # Stop streaming
  end: ->
    @disconnecting = true
    if @request
      console.log('node-flowdock: steam->end')
      @request.abort()
      @request.removeAllListeners()
      @request = undefined
  close: ->
    console.warn 'DEPRECATED, use Stream#end() instead'
    @end()

# Connect to flows
Stream.connect = (auth, flows, params) ->
  console.log('node-flowdock: steam->connect')
  stream = new Stream auth, flows, params
  stream.connect()
  stream


Stream.backoff =
  network:
    delay: 200
    max: 10000
  error:
    delay: 2000
    max: 120000

module.exports = Stream
