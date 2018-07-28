package org.softwood.base

import groovyx.gpars.agent.Agent
import org.nustaq.serialization.FSTConfiguration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.zeromq.ZContext
import org.zeromq.ZFrame
import org.zeromq.ZMQ
import org.zeromq.ZMsg

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

final enum GzmqEndpointType {
    CLIENT,
    SERVER
}

/**
 * Created by willw on 30/06/2017.
 *
 * implemented as a trait, can be implemented by any client class
 * essentialy builds a ZMQ socket of required type and maintains access
 * to the socket through GPARs Agent.  Uses higher level API ZContext to
 * create sockets.  ZContext maintains mutux List of sockets created in
 * this context instance
 * assumes instance of client class has been created either by new, or factory method
 *
 * logically a Gzmq instance represents a 0mq socket, of endpoint type one of client or server
 *
 */

trait GzmqTrait {

    static final int DEFAULT_POOL_SIZE = 1
    static final String DEFAULT_PROTOCOL = 'tcp'
    static final String DEFAULT_HOST = 'localhost'
    static final long DEFAULT_PORT = 5555
    static final String DEFAULT_SUBSCRIBE_ALL = ''

    /**
     * internal initial default options map using above.  Instance is
     * final but values may be added/removed
     */
    private final Map defaultOptionsMap = [
            poolSize: DEFAULT_POOL_SIZE,
            protocol:DEFAULT_PROTOCOL,
            host:DEFAULT_HOST,
            port: DEFAULT_PORT,
            subscription: DEFAULT_SUBSCRIBE_ALL]

    //We cant use AST transformations in traits - so setup a logger the old fashioned way
    Logger log = LoggerFactory.getLogger(this.getClass())

    /**
     * user high level api
     * context is thread safe, sockets are not
     * It manages open sockets in the context and automatically closes these before terminating the context
     * Sets-up signal (interrupt) handling for the process.
     */
    static ZContext context  //holds list of sockets it creates

    ConcurrentLinkedQueue errors = []

    //make socket thread safe
    //todo remove single socket cosntraint - Gzmq instace can have more than 1 socket?
    Agent<ZMQ.Socket> socketAgent = new Agent(null)
    Agent<ZMsg> lastSentMessageHeadersAgent = new Agent (new ZMsg())
    GzmqEndpointType endpointType  //needs to be thread safe ?

    //setup codecs for various serialisation options using FST library
    //codecs map sets up closures for each serialisation type
    Closure encode
    Closure decode
    static jsonConf = FSTConfiguration.createJsonConfiguration(true,false)
    static javaConf = FSTConfiguration.createDefaultConfiguration()
    static minBinConf = FSTConfiguration.createMinBinConfiguration()
    static final Map codecs = [json: [encode: jsonConf.&asByteArray, decode: jsonConf.&asObject],
                                java: [encode: javaConf.&asByteArray, decode: javaConf.&asObject],
                                minBin: [encode: minBinConf.&asByteArray, decode: minBinConf.&asObject],
                                none: [encode : {it}], decode: {it} ]

    //by default codecs are not assumed so values will be passed to
    //send methods as is - therefore types must support getByteArray method
    AtomicBoolean codecEnabled = new AtomicBoolean(false)

    Timer timer
    int delay, frequency, finish
    volatile boolean timerEnabled = false  //ensure we cross memory boundary

    /**
     * as traits don't support annotations for logging, provide
     * a setter to let the class that implements the trait to inject
     * its log instance for the trait to use
     * @param injectedLog
     * @return
     */
    def setLogger (injectedLog) {
        assert injectedLog
        log = injectedLog
    }

    /**
     * constructs a connection address by concatenating
     * @param protocol - typically tcp assumed
     * @param host
     * @param port
     * @param options - map of any ovveride options
     * @return formatted connection string
     */
    private String _getConnectionAddress ( String protocol = "tcp", String host = "localhost", def port = 5555, Map options = [:]) {

        def socketProtocol = protocol
        def socketHost = host
        def socketPort = port

        if (options.hasProperty("protocol"))
            socketProtocol = options.protocol  // override from options if it exists
        if (options.hasProperty("host"))
            socketProtocol = options.host  // override from options if it exists
        if (options.hasProperty("port"))
            socketPort = options.port  // override from options if it exists

        //build connection address string
        return "${socketProtocol}://${socketHost}:${socketPort}"  //todo regex processing for host string
    }

    // making this private causes runtime error on interface types!? Weird
    GzmqEndpointType _getDefaultEndpointForSocketType(String socketType, Map options = [:]){
        GzmqEndpointType endpointType

        //check if and endpoint type override (client or server) is suggested for the socket
        GzmqEndpointType overrideEndpointType = options.endpoint

        //otherwise return a sensible default endpoint type for a given socketType
        if (!overrideEndpointType){

            switch  (socketType.toUpperCase())  {
                case "REQ" : endpointType = GzmqEndpointType.CLIENT ;  break
                case "REP" : endpointType = GzmqEndpointType.SERVER ;  break  //XREP
                case "PUB" : endpointType = GzmqEndpointType.CLIENT ; break
                case "SUB" : endpointType = GzmqEndpointType.SERVER ; break
                case "DEALER" :  endpointType = GzmqEndpointType.CLIENT; break
                case "ROUTER" : endpointType = GzmqEndpointType.SERVER; break
                case "PUSH" : endpointType = GzmqEndpointType.CLIENT ;  break
                case "PULL" : endpointType = GzmqEndpointType.SERVER;   break
                //todo case "PAIR" : sockType = ZMQ.PAIR; break
                default : endpointType = GzmqEndpointType.CLIENT; break
            }
        } else
            endpointType = overrideEndpointType
        endpointType
    }


     /**
     * if no context exists - it will create one and store it on the class instance in the trait.
     *
     * The _createConnection function will build and configure a socket of the appropriate named type and store that
     * in an agent to assure thread safety for the socket.  ZContext also stores list of contexts created on  it
     * Need to look very carefully at the whole thread journey and not get things mixed
     *
     * interface is functionally oriented so that calls can be chained
     *
     * @param socketType - string of type of socket you want to create - automatically sets assumed endpoint type for the socket  of 'client' or 'server'
     * @param protocol
     * @param host
     * @param port
     * @param options - map of overidde values should any be provided
     * @return
     */
    private def _createConnection (String socketType, String protocol = "tcp", String host = "localhost", def port = 5555, Map options = [:]) {
        int poolSize = options.'poolSize' ?: defaultOptionsMap.poolSize
        if (!context) {
            context = new ZContext (poolSize)
        }

        if (port instanceof String)
            port = Long.parseLong(port)

        //no log on impl cl\ass log.debug "_createConnection: configure connection for socket "
        def connectionAddress = _getConnectionAddress(protocol, host, port, options)  //todo regex processing for host string

        def sockType
        switch (socketType.toUpperCase()) {
            case "REQ" : sockType = ZMQ.REQ ; break
            case "REP" : sockType = ZMQ.REP;  break;  //XREP
            case "PUB" : sockType = ZMQ.PUB;  break
            case "SUB" : sockType = ZMQ.SUB;  break
            case "DEALER" : sockType = ZMQ.DEALER; break
            case "ROUTER" : sockType = ZMQ.ROUTER; break
            case "PULL" : sockType = ZMQ.PULL;  break
            case "PUSH" : sockType = ZMQ.PUSH;  break
            case "PAIR" : sockType = ZMQ.PAIR; break
        }
        //if endpoint is declared override
        endpointType = _getDefaultEndpointForSocketType(socketType, (Map)options)

       //get the socket
        //ZMQ.Socket socketInstance = context.createSocket(sockType)
        ZMQ.Socket socketInstance = context.createSocket(sockType)

        String identity = options.'identity'
        if (identity)
            socketInstance.setIdentity(identity.getBytes())
        println "socket of type $socketType identity is " + socketInstance.getIdentity().toString()
        socketAgent.updateValue(socketInstance)  // is this not supposed to send message to effect this.

        //if SUB socket setup subscription to topics or get all if none defined
        if (sockType == ZMQ.SUB || sockType == ZMQ.XSUB) {
            def topics = options.'topics' ?: [DEFAULT_SUBSCRIBE_ALL]
            topics.each {socketAgent << {it.subscribe(it)}}
        }

        //and either 'connect' as client or 'bind' as server.  default of 'client: connect' is assumed
        switch (endpointType) {
            case GzmqEndpointType.CLIENT : socketAgent << {it.connect (connectionAddress)}; break
            case GzmqEndpointType.SERVER : socketAgent << {it.bind (connectionAddress)}; break
            default: socketAgent <<{it.connect (connectionAddress)} ; break
        }
        this

    }

    void clearErrors() {
        errors.clear()
    }

    boolean hasErrors () {
        if (errors.size() == 0)
            false
        else
            true
    }

    def configure (String socketType, options = [:]){

    }

    /***
     * delegates to based full fledged version, assumes its called with a socketType in the map
     * @param details
     * @param doWork
     * @return
     */
    def withGzmq (Map details = [:], Closure doWork) {
        println "details map : " + details
        def socketType = details.'socketType'
        assert socketType
        details.remove('socketType')
        def protocol = details.'protocol' ?: DEFAULT_PROTOCOL
        details.remove ('protocol')
        def port = details.'port' ?: DEFAULT_PORT
        details.remove ('port')
        def host = details.'host' ?: DEFAULT_HOST
        details.remove ('host')

        withGzmq (socketType, protocol, host, port, details,  doWork )
    }

    /**
     * withGzmq will build a new socket instance and use it when running the doWork closure
     * and then close the socket down.
     * This is one time used only
     * @param socketType
     * @param protocol
     * @param host
     * @param port
     * @param options
     * @param doWork
     * @return
     */
    def withGzmq (String socketType, String protocol = "tcp", String host = "localhost", def port = 5555, Map options, Closure doWork) {
        assert doWork instanceof Closure

        if (port instanceof String)
            port = Long.parseLong(port)

        _createConnection(socketType, protocol, host, port, options)

        Closure work = doWork?.clone()
        work?.delegate = this    //set Gzmq as delegate for the closure

        errors.clear()
        try {
            //invoke closure with gzmqTrait instance 'socket'
            work?.call(this)
        } catch (Exception e) {
            errors << e.toString()  //todo define error object - quick cheat
            log.debug "withSocket : work closure /runnable produced error with \n " + e.printStackTrace()
        } finally {
            log.debug "withSocket: close connection"
            _tidyAndDestroySocket(this)
            //todo - set socket to null ?
        }//end finally

        this
    }

    //used for method chaining
    def codec (type) {
        def codecType
        switch (type.toLowerCase()) {
            case 'json' : codecType = 'json'; break
            case 'java' : codecType = 'java';break
            case 'minBin': codecType = 'minBin';break
            case 'none': codecType = 'none'; break
            default: codecType = 'java'; break
        }

        encode = (codecs[codecType])['encode']
        decode = (codecs[codecType])['decode']
        codecEnabled.getAndSet( (type != 'none') ? true : false )
        this
    }

    ZMQ.Socket getSocket() {
        socketAgent.val
    }

    def close () {
        clearErrors()
        if (context != null ) {
            println "close called "
            _tidyAndExit(this)
            if (context.isClosed())
                context = null
            if (timer)
                timer.cancel()
        }

        this
    }

    //  internal routine: close zmq socket and terminate the context
    private Closure _tidyAndExit (GzmqTrait gzmq) {
        assert gzmq
        ZContext zcontext = gzmq.context
        def socket = socketAgent.val
        socketAgent << {it.disconnect(); it.close()}
        socketAgent.updateValue(null)//clear old socket

        zcontext.destroySocket (socket)  //todo is this double effort ?

        zcontext.close()
    }

    //  internal routine: close zmq socket and terminate the context
    private Closure _tidyAndDestroySocket (GzmqTrait gzmq) {
        assert gzmq
        ZContext zcontext = gzmq.context
        def socket = socketAgent.val
        socketAgent << {it.disconnect(); it.close()}
        socketAgent.updateValue(null)//clear old socket

        zcontext.destroySocket (socket)  //todo is this double effort ?
    }

    byte[] _getBytesToSend(closureToEval) {/*= {clos -> *?*/
        def buffer
        if (codecEnabled)
            buffer = encode(closureToEval())  //encode returned result of closure
        else {
            buffer = closureToEval.call()
            if (buffer instanceof GString) {
                buffer = buffer.bytes
            }
            else if (buffer instanceof String) {
                buffer = GString.newInstance (buffer).bytes
            }
            else {
                FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

                // get object as serialised byte array
                buffer = conf.asByteArray(buffer)
                //println "cant convert to bytes"
            }
        }
        buffer
    }

    def scheduledSend (message, delay, frequency, finish=0) {
        def buf

        Timer newTimer = new Timer()  //thread safety later
        this.timer = newTimer

        Closure dataClosure
        if (message instanceof Closure) {
            dataClosure = message
            def result = message()
            println "schSend : result of passed closure was " + result
        }
        else
            dataClosure = {message}

        if (finish) {
            //log.debug "setup timer cancellation in $finish ms"
            timer.schedule({timer.cancel(); println "sch timer cancelled"}, finish)
                   //schedule timer cancellation if finish defined
        }

        assert socketAgent.val

        String first = new String (_getBytesToSend(dataClosure))

        println "first set of bytes > $first"
        timer.schedule (
                //task, evaluate closure each time in case result is different
                //{socket.send (codecEnabled ? encode (dataClosure()): dataClosure() as byte[], 0)},
                {socketAgent << {it.send (_getBytesToSend(dataClosure), 0)}},
                delay,
                frequency)
        println "sch socket send every $frequency ms"

        this
    }

    /**
     * sends a message to socket using higher level API
     * could take optional headers map - and create a Frame for each header in the map,
     * tags the message as last frame using codec if defined
     * @param message
     * @return
     */
    def send (message) {
        def buf
        if (message.class == Closure)
            message = message()  // call closure and use result as object to send
        if (codecEnabled.get()) {
            buf = encode (message)
            println "send: encoded message as $buf"
        } else {
            buf = message as byte[]
            println "send: unencoded message as $buf"
        }

        assert socketAgent.val

        /**
         * create new message with no 0mq headers - just message delimeter and data as last frame
         */
        Agent<ZMsg> outMsgAgent = new Agent (new ZMsg())
        outMsgAgent << {it.add (new ZFrame()) }  //send empty frame delimiter
        outMsgAgent << {it.add (new ZFrame (buf))}

        //def result = socketAgent << {it.send (buf, 0) }  //original code before using ZMsg
        //new : try sending message as sequence of frames
        def result = socketAgent << {socket -> outMsgAgent.val.send(socket) }
        this
    }

    /**
     * allows receiver (normally a server) respond to a message.  There can be many senders on
     * fan in so you have to be able to capture and save which client called you
     * @param clientAddress
     * @param message
     * @return
     */
    def reply (message, ZMsg clientAddress = null) {
        def buf
        if (message.class == Closure)
            message = message()  // call closure and use result as object to send
        if (codecEnabled.get()) {
            buf = encode (message)
            println "send: encoded message as $buf"
        } else {
            buf = message as byte[]
            println "send: unencoded message as $buf"
        }

        assert socketAgent.val

        ZMsg client = clientAddress ?: lastSentMessageHeadersAgent.val  //if clientAddress is null use last record message

        //setup the return message with correct 0mq headers
        Agent<ZMsg> repMsgAgent = new Agent (new ZMsg())
        //clientAddress.each {frame -> outMsg << frame}  //setup client return address details
        repMsgAgent << {it.addAll(client.toArray())}
        repMsgAgent << {it.add new ZFrame (buf)}

        def result = socketAgent << {socket -> repMsgAgent.val.send(socket) }

        //had a concurrent modification exception here so protected repMsg
        println "sent repy message to "
        repMsgAgent << {it.dump (System.out)}

        //TODO: check result return for error
        this
    }

    //delegate to full form with null class 'type' expectation
    def receive (Closure resultCallback, Closure sentFromAddress = null) {
        return receive (null, resultCallback, sentFromAddress)
    }

    def receive (Class type, Closure resultCallback, Closure sentFromAddress = null) {
        assert resultCallback
        assert socketAgent.val
        byte[] result = []
        ZMsg resultMsg

        socketAgent.sendAndWait {resultMsg = ZMsg.recvMsg (it)}  //wait for result to be set, theres a wait timeout version
        if (resultMsg == null) {
            println "receive message barfed "
            //todo create errors object
        }

        ZFrame dataFrame = resultMsg.removeLast()
        result = dataFrame.getData()

        //lastReceiveMessageHeaders
        ZMsg sentFromHeaders = new ZMsg()
        //get all headers and delimiter frame and set in the sentFrom
        def frames = resultMsg.toArray()
        sentFromHeaders.addAll(resultMsg.toArray())
        lastSentMessageHeadersAgent.updateValue (sentFromHeaders)

        if (sentFromAddress instanceof Closure){

            sentFromAddress (sentFromHeaders)  //call closure and pass in the sentFromHeaders
            println "setup return address as "
            sentFromHeaders.dump (System.out)
        }

        println "receive: result as bytes : ${result.toString()} (${String.newInstance(result)})"
        if (codecEnabled.get()) {
            resultCallback (decode (result))
        } else {
            if (type == String){
                println "receive: requested return type was String, convert result to String"
                log.debug "receive: requested return type was String, convert result to String"
                resultCallback (String.newInstance(result))
            } else if (type != null)
                resultCallback (result.asType(type))
             else
                resultCallback (result)
        }
        this
    }

    /**
     * supports passing the receive header frames to passed closure
     *
     * @param clientHeaders - closure called with receive's header frames
     * @return
     */
    def rightShift (Closure clientHeaders) {

        assert clientHeaders

        Closure clientHeaderClosure = clientHeaders.clone()
        clientHeaderClosure.delegate = this //set Gzmq as delegate

        //call closure passing in the header frames retrieved from the receive call
        clientHeaderClosure.call (lastSentMessageHeadersAgent.val)
        this
    }


    //request reply model

    def requestConnection (connectionAddress = "", Map options = [:],  Closure doWork=null) {
        def poolSize = options.'poolSize' ?: defaultOptionsMap.poolSize
        if (!context)
            context = new ZContext(poolSize)


        def socketType = options.'socketType'
        //assert socketType
        options.remove('socketType')
        def protocol = options.'protocol' ?: DEFAULT_PROTOCOL
        options.remove ('protocol')
        def port = options.'port' ?: DEFAULT_PORT
        options.remove ('port')
        def host = options.'host' ?: DEFAULT_HOST
        options.remove ('host')

        println "requesting connection to the zmq server"
        ZMQ.Socket requester = context.createSocket(ZMQ.REQ)
        //TODO set socket options
        requester.connect(connectionAddress ?: "tcp://localhost:5555")
        socketAgent.updateValue(requester)

        //invoke closure with requester socket
        if (doWork) {
            println "doing request work in closure "
            doWork(this)
        }

        //check options flag for complete key, if so invoke exit closure
        if (options.complete) {
            println "complete option was set on request - tidy and get out"
            _tidyAndDestroySocket(this)
        }

        this
    }

    def replyConnection (connectionAddress = "", Map options = [:], Closure doWork=null) {
        def poolSize = options.'poolSize' ?: defaultOptionsMap.poolSize
        if (!context)
            context = new ZContext(poolSize)

        println "server binding on REP port "
        ZMQ.Socket responder  = context.createSocket(ZMQ.REP)
        responder.bind(connectionAddress ?: "tcp://localhost:5555")
        socketAgent.updateValue(responder)

        //invoke closure with requester socket
        if (doWork) {
            println "doing reply work in closure "
            doWork (this )
        }

        //check options flag for complete key, if so invoke exit closure
        if (options.complete) {
            println "complete option was set on reply  - tidy and get out"
            _tidyAndDestroySocket(this)
        }

        this
    }


    //pub sub model
    def publisherConnection (connectionAddress = [""], Map options = [:]) {
        def poolSize = options.'poolSize' ?: defaultOptionsMap.poolSize
        if (!context)
            context = new ZContext(poolSize)

        errors.clear()

        def port = options.'port' ?: "5556"
        def protocol = options.'protocol' ?: 'tcp'
        def host = options.'host' ?: 'localhost'
        if (connectionAddress == [""])
            connectionAddress = ["${protocol}://${host}:${port}"]


        println "publisher (server) binding on publisher connection : $connectionAddress "
        ZMQ.Socket publisher

        try {
            publisher = context.createSocket (ZMQ.PUB)
            if (connectionAddress.size() == 1) {
                publisher.bind(connectionAddress[0])
            }
            else {
                connectionAddress.each {publisher.bind (it)}
            }
            //publisher.bind ("ipc://weather")  //figure out to handle multiple binds
            socketAgent.updateValue(publisher)

            this
        } catch (Exception e) {
            errors << "method:publisherConnection : " + e.toString()
            println e.printStackTrace()
        } finally {
            return this
        }

    }

    def subscriberConnection (topics, connectionAddress = [""], Map options = [:]) {
        def poolSize = options.'poolSize' ?: defaultOptionsMap.poolSize
        if (!context)
            context = new ZContext(poolSize)

        errors.clear()

        if (connectionAddress == [""])
            connectionAddress = _getConnectionAddress(defaultOptionsMap.protool, defaultOptionsMap.host, defaultOptionsMap.port, options )

        println "subscriber (client) connecting to publishing port "
        ZMQ.Socket subscriber
        subscriber = socketAgent.val
        if (!subscriber)
            subscriber = context.createSocket (ZMQ.SUB)  //create new subscriber socket
        connectionAddress.each {subscriber.connect (it)}
        topics.each {subscriber.subscribe (it.bytes)}

        //publisher.bind ("ipc://weather")  //figure out to handle multiple binds
        socketAgent.updateValue(subscriber)

        this

    }

    def subscribe (topics) {
        println "subscribe to topic $topics "
        if (topics == "") {
            socketAgent << {it.subscribe (topics.bytes)}     //subscribe all messages
        }
        else if (! topics instanceof Iterable ) {
            socketAgent << {it.subscribe (topics.bytes)}
        }
        else {
            //for each element
            topics.each {socketAgent << {it.subscribe (it.bytes)} }
        }
        //publisher.bind ("ipc://weather")  //figure out to handle multiple binds

        this

    }

    def unsubscribe (topics) {
        println "unsubscribe to topic $topics "
        ZMQ.Socket subscriber = socketAgent.val
        if (topics == "") {
            socketAgent << {it.unsubscribe(topics.bytes)}     //subscribe all messages
        }
        else if (! topics instanceof ArrayList ) {
            socketAgent << {it.unsubscribe(topics.bytes)}
        }
        else {
            //for each element
            topics.each { socketAgent << {it.unsubscribe(topics.bytes)} }
        }
        //publisher.bind ("ipc://weather")  //figure out to handle multiple binds

        this

    }
}
