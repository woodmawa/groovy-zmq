package org.softwood.base

import groovy.util.logging.Slf4j
import groovyx.gpars.agent.Agent
import org.nustaq.serialization.FSTConfiguration
import org.zeromq.ZContext
import org.zeromq.ZMQ

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
 */
@Slf4j
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

    /**
     * user high level api
     * context is thread safe, sockets are not
     * It manages open sockets in the context and automatically closes these before terminating the context
     * Sets-up signal (interrupt) handling for the process.
     */
    ZContext context  //holds list of sockets it creates

    ConcurrentLinkedQueue errors = []

    //make socket thread safe
    //todo remove single socket cosntraint - Gzmq instace can have more than 1 socket?
    Agent<ZMQ.Socket> clientSocketAgent = new Agent(null)
    Agent<ZMQ.Socket> serverSocketAgent = new Agent(null)

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

    private GzmqEndpointType _getDefaultEndpointSocketType (String socketType, Map options = [:]){
        def endpointType

        def overrideEndpointType = options.endpoint

        if (!overrideEndpointType){

            switch  (socketType.toUpperCase())  {
                case "REQ" : endpointType = GzmqEndpointType.CLIENT ;  break
                case "REP" : endpointType = GzmqEndpointType.SERVER ;  break  //XREP
                case "PUB" : endpointType = GzmqEndpointType.CLIENT ; break
                case "SUB" : endpointType = GzmqEndpointType.SERVER ; break
                case "PULL" : endpointType = GzmqEndpointType.SERVER;   break
                case "PUSH" : endpointType = GzmqEndpointType.CLIENT ;  break
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
            case "REQ" : sockType = ZMQ.REQ ;  break
            case "REP" : sockType = ZMQ.REP;  break;  //XREP
            case "PUB" : sockType = ZMQ.PUB; break
            case "SUB" : sockType = ZMQ.SUB; break;
            case "PULL" : sockType = ZMQ.PULL;   break;
            case "PUSH" : sockType = ZMQ.PUSH;  break
            case "PAIR" : sockType = ZMQ.PAIR; break
        }
        //if endpoint is declared override
        def endpointType = _getDefaultEndpointSocketType(socketType)

        def optEndpoint = options.'endpointType'  //should be enum type
        def endpoint =  optEndpoint ?: endpointType //default for a client - i.e connect socket

        //get the socket
        //ZMQ.Socket socketInstance = context.createSocket(sockType)
        ZMQ.Socket socketInstance = context.createSocket(sockType)

        String identity = options.'identity'
        if (identity)
            socketInstance.setIdentity(identity.getBytes())
        println "socket of type $socketType identity is " + socketInstance.getIdentity().toString()
        if (endpoint == GzmqEndpointType.CLIENT)
            clientSocketAgent.updateValue(socketInstance)  // is this not supposed to send message to effect this.
        else if (endpoint == GzmqEndpointType.SERVER)
            serverSocketAgent.updateValue(socketInstance)

        //if SUB socket setup subscription to topics or get all if none defined
        if (sockType == ZMQ.SUB || sockType == ZMQ.XSUB) {
            def topics = options.'topics' ?: [DEFAULT_SUBSCRIBE_ALL]
            topics.each {clientSocketAgent << {it.subscribe(it)}}
        }

        //and either 'connect' as client or 'bind' as server.  default of 'connect' is assumed
        switch (endpoint) {
            case GzmqEndpointType.CLIENT : clientSocketAgent << {it.connect (connectionAddress)}; break
            case GzmqEndpointType.SERVER : serverSocketAgent << {it.bind (connectionAddress)}; break
            default: clientSocketAgent <<{it.connect (connectionAddress)} ; break
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
        work.delegate = this    //set Gzmq as delegate for the closure

        errors.clear()
        try {
            //invoke closure with gzmqTrait instance 'socket'
            work?.call(this)
        } catch (Exception e) {
            errors << e.toString()  //todo define error object - quick cheat
            log.debug "withSocket : errored with " + e.printStackTrace()
        } finally {
            log.debug "withSocket: close connection"
            def endpointType = _getDefaultEndpointSocketType(socketType)
            if (endpointType == GzmqEndpointType.CLIENT) {
                clientSocketAgent << { it.disconnect(); it.close() }
                clientSocketAgent.updateValue(null)//clear old socket
            } else {
                serverSocketAgent.updateValue(null)//clear old socket
                serverSocketAgent.updateValue(null)//clear old socket
            }
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
        clientSocketAgent.val
    }

    def close () {
        clearErrors()
        if (context != null ) {
            println "close called "
            _tidyAndExit(this)
            clientSocketAgent.updateValue(null)
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
        def socket = clientSocketAgent.val
        clientSocketAgent << {it.disconnect(); it.close()}
        clientSocketAgent.updateValue(null)//clear old socket

        zcontext.destroySocket (socket)  //todo is this double effort ?

        zcontext.close()
    }

    byte[] _getBytes (closToEval) {/*= {clos -> *?*/
        def buffer
        if (codecEnabled)
            buffer = encode(closToEval())  //encode returned result of closure
        else {
            buffer = closToEval.call()
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

        assert clientSocketAgent.val

        String first = new String (_getBytes(dataClosure))

        println "first set of bytes > $first"
        timer.schedule (
                //task, evaluate closure each time in case result is different
                //{socket.send (codecEnabled ? encode (dataClosure()): dataClosure() as byte[], 0)},
                {clientSocketAgent << {it.send (_getBytes(dataClosure), 0)}},
                delay,
                frequency)
        println "sch socket send every $frequency ms"

        this
    }


    def send (message) {
        def buf
        if (message.class == Closure)
            message = message()  // call closure and use result as object to send
        if (codecEnabled) {
            buf = encode (message)
            println "send: encoded message as $buf"
        } else {
            buf = message as byte[]
        }

        assert clientSocketAgent.val

        def result = clientSocketAgent << {it.send (buf, 0) }//block till message arrives

        //TODO: check result return for error
        this
    }

    def receive (Class type = null, Closure resultCallback) {
        assert resultCallback
        assert clientSocketAgent.val
        byte[] result = []
        clientSocketAgent.sendAndWait {result = it.recv()}  //wait for result to be set
        println "receive: result as bytes : ${result.toString()} (${String.newInstance(result)})"
        if (codecEnabled) {
            resultCallback (decode (result))
        } else {
            if (type == String){
                println "receive: expected type was String, convert result to String"
                resultCallback (String.newInstance(result))
            } else if (type != null)
                resultCallback (result.asType(type))
             else
                resultCallback (result)
        }
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
        clientSocketAgent.updateValue(requester)

        //invoke closure with requester socket
        if (doWork) {
            println "doing request work in closure "
            doWork(this)
        }

        //check options flag for complete key, if so invoke exit closure
        if (options.complete) {
            println "complete option was set on request - tidy and get out"
            _tidyAndExit(this)
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
        serverSocketAgent.updateValue(responder)

        //invoke closure with requester socket
        if (doWork) {
            println "doing reply work in closure "
            doWork (this )
        }

        //check options flag for complete key, if so invoke exit closure
        if (options.complete) {
            println "complete option was set on reply  - tidy and get out"
            _tidyAndExit(this)
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
            clientSocketAgent.updateValue(publisher)

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
        subscriber = clientSocketAgent.val
        if (!subscriber)
            subscriber = context.createSocket (ZMQ.SUB)  //create new subscriber socket
        connectionAddress.each {subscriber.connect (it)}
        topics.each {subscriber.subscribe (it.bytes)}

        //publisher.bind ("ipc://weather")  //figure out to handle multiple binds
        clientSocketAgent.updateValue(subscriber)

        this

    }

    def subscribe (topics) {
        println "subscribe to topic $topics "
        if (topics == "") {
            clientSocketAgent << {it.subscribe (topics.bytes)}     //subscribe all messages
        }
        else if (! topics instanceof Iterable ) {
            clientSocketAgent << {it.subscribe (topics.bytes)}
        }
        else {
            //for each element
            topics.each {clientSocketAgent << {it.subscribe (it.bytes)} }
        }
        //publisher.bind ("ipc://weather")  //figure out to handle multiple binds

        this

    }

    def unsubscribe (topics) {
        println "unsubscribe to topic $topics "
        ZMQ.Socket subscriber = clientSocketAgent.val
        if (topics == "") {
            clientSocketAgent << {it.unsubscribe(topics.bytes)}     //subscribe all messages
        }
        else if (! topics instanceof ArrayList ) {
            clientSocketAgent << {it.unsubscribe(topics.bytes)}
        }
        else {
            //for each element
            topics.each { clientSocketAgent << {it.unsubscribe(topics.bytes)} }
        }
        //publisher.bind ("ipc://weather")  //figure out to handle multiple binds

        this

    }
}
