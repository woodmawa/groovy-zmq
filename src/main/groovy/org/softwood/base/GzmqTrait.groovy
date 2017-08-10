package org.softwood.base

import groovy.util.logging.Slf4j
import groovyx.gpars.agent.Agent
import org.nustaq.serialization.FSTConfiguration
import org.zeromq.ZContext
import org.zeromq.ZMQ
import org.zeromq.ZSocket
import sun.security.pkcs11.Secmod

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue


/**
 * Created by willw on 30/06/2017.
 *
 * implemented as a trait, can be implemented in any client class
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

    //user high level api
    //ZMQ.Context context  //context is thread safe, sockets are not
    ZContext context  //holds list of sockets it creates

    ConcurrentLinkedQueue errors = []

    //make socket thread safe
    //Agent<ZMQ.Socket> socketAgent = new Agent(null)
    Agent<ZMQ.Socket> socketAgent = new Agent(null)
    Closure encode
    Closure decode
    static jsonConf = FSTConfiguration.createJsonConfiguration(true,false)
    static javaConf = FSTConfiguration.createDefaultConfiguration()
    static minBinConf = FSTConfiguration.createMinBinConfiguration()
    static final Map codecs = [json: [encode: jsonConf.&asByteArray, decode: jsonConf.&asObject],
                                java: [encode: javaConf.&asByteArray, decode: javaConf.&asObject],
                                minBin: [encode: minBinConf.&asByteArray, decode: minBinConf.&asObject],
                                none: [encode : {it}], decode: {it} ]
    volatile boolean codecEnabled = false
    Timer timer
    int delay, frequency, finish
    volatile boolean timerEnabled = false

    def configure (String socketType, String protocol = "tcp", String host = "localhost", String port = "5555", Map options = [:]) {
        long poolSize = options.'poolSize' ?: DEFAULT_POOL_SIZE
        if (!context)
            //context = ZMQ.context(poolSize)
            context = new ZContext (poolSize)

        println "configure connection for socket "
        def connectionAddress = "${protocol}://${host}:${port}"  //todo regex processing for host string
        def sockType
        def endpoint = ""
        switch (socketType) {
            case "REQ" : sockType = ZMQ.REQ ; endpoint = "client"; break
            case "REP" : sockType = ZMQ.REP; endpoint = "server"; break;  //XREP
            case "PUB" : sockType = ZMQ.PUB; endpoint = "client";break
            case "SUB" : sockType = ZMQ.SUB; endpoint = "server";break;
            case "PULL" : sockType = ZMQ.PULL; endpoint = "client";  break;
            case "PUSH" : sockType = ZMQ.PUSH; endpoint = "server"; break
            case "PAIR" : sockType = ZMQ.PAIR; break
        }
        //if endpoint is declared override
        def optEndpoint = options.'endpoint'
        endpoint =  optEndpoint ?: 'client'  //default to client - i.e connect socket

        //get the socket
        //ZMQ.Socket socketInstance = context.createSocket(sockType)
        ZMQ.Socket socketInstance = context.createSocket(sockType)
        //and either connect as client or as server
        switch (endpoint) {
            case "client" : socketInstance.connect (connectionAddress); break
            case "server" : socketInstance.bind (connectionAddress); break
            default: socketInstance.connect (connectionAddress); break
        }
        socketAgent.updateValue(socketInstance)
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

    def withSocket (Map details = [:], Closure doWork) {
        println "details map : " + details
        def socketType = details.'socketType'
        assert socketType
        details.remove('socketType')
        def protocol = details.'prototype' ?: DEFAULT_PROTOCOL
        details.remove ('prototype')
        def port = details.'port' ?: DEFAULT_PORT
        details.remove ('port')
        def host = details.'host' ?: DEFAULT_HOST
        details.remove ('host')

        withSocket (socketType, protocol, host, port, details,  doWork )
    }


    def withSocket (String socketType, String protocol = "tcp", String host = "localhost", String port = "5555", Map options, Closure doWork) {
        assert doWork instanceof Closure
        int poolSize = options.'poolSize' ?: DEFAULT_POOL_SIZE
        if (!context)
            context = new ZContext(DEFAULT_POOL_SIZE)  //ZMQ.context(DEFAULT_POOL_SIZE)

        errors.clear()
        String connectionAddress = "${protocol}://${host}:${port}".toString()  //todo regex processing for host string
        def sockType
        def endpoint = ""
        switch (socketType) {
            case "REQ" : sockType = ZMQ.REQ ; endpoint = "client"; break
            case "REP" : sockType = ZMQ.REP; endpoint = "server"; break;  //XREP
            case "PUB" : sockType = ZMQ.PUB; endpoint = "server";break
            case "SUB" : sockType = ZMQ.SUB; endpoint = "client";break;
            case "PULL" : sockType = ZMQ.PULL; endpoint = "client";  break;
            case "PUSH" : sockType = ZMQ.PUSH; endpoint = "server"; break
            case "PAIR" : sockType = ZMQ.PAIR; break
            default : errors << "invalid socketType $socketType"
        }

        ZMQ.Socket socketInstance
        socketInstance = socketAgent.val
        if (!socketInstance) {
            //get the socket and save it in agent wrapper, including any topic subscriptions if SUB/XSUB
            try {
                socketInstance = context.createSocket (sockType)// context.createSocket(sockType)
                assert socketInstance
                socketAgent.updateValue(socketInstance)


                //and either connect as client or as server
                switch (endpoint) {
                    case "client":
                        println "withSocket: connecting to $connectionAddress "
                        socketAgent << {it.connect(connectionAddress)}
                        break
                    case "server":
                        println "withSocket: binding to $connectionAddress "
                        socketAgent << {it.bind(connectionAddress)}
                        break
                    default:
                        println "withSocket: [default] connecting to $connectionAddress "
                        socketAgent << {it.connect(connectionAddress)}
                        break
                }

                //if SUB socket setup subscrition to topics or get all if none defined
                if (sockType == ZMQ.SUB || sockType == ZMQ.XSUB) {
                    def topics = options.'topics' ?: [DEFAULT_SUBSCRIBE_ALL]
                    topics.each {socketAgent << {it.subscribe(it)}}
                }

                //invoke closure with gzmqTrait instance 'socket'
                doWork?.call(this)

            } catch (Exception e) {
                errors << e.toString()  //todo define error object - quick cheat
                println e.printStackTrace()
            } finally {
                //println "withSocket: close connection"
                //this.close()
            }
        } else {
            try {
                //invoke closure with gzmqTrait instance 'socket'
                doWork?.call(this)

            } catch (Exception e) {
                errors << e.toString()  //todo define error object - quick cheat
                println e.printStackTrace()
            } finally {
                //println "withSocket: close connection"
                //this.close()
            }
        }

        this
    }

    def codec (type) {
        def codecType
        switch (type) {
            case 'json' : codecType = 'json'; break
            case 'java' : codecType = 'java';break
            case 'minBin': codecType = 'minBin';break
            case 'none': codecType = 'none'; break
            default: codecType = 'json'; break
        }

        encode = (codecs[codecType])['encode']
        decode = (codecs[codecType])['decode']
        codecEnabled = (type != 'none') ? true : false
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
            socketAgent.updateValue(null)
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
        ZMQ.Socket socket = gzmq.socketAgent.val
        zcontext.destroySocket (socket)

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

        assert socketAgent.val

        String first = new String (_getBytes(dataClosure))

        println "first set of bytes > $first"
        timer.schedule (
                //task, evaluate closure each time in case result is different
                //{socket.send (codecEnabled ? encode (dataClosure()): dataClosure() as byte[], 0)},
                {socketAgent << {it.send (_getBytes(dataClosure), 0)}},
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

        assert socketAgent.val

        def result = socketAgent << {it.send (buf, 0) }//block till message arrives

        //TODO: check result return for error
        this
    }

    def receive (Class type = null, Closure resultCallback) {
        assert resultCallback
        assert socketAgent.val
        byte[] result = []
        socketAgent.sendAndWait {result = it.recv()}  //wait for result to be set
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
        def poolSize = options.'poolSize' ?: DEFAULT_POOL_SIZE
        if (!context)
            context = new ZContext(poolSize)

        def socketType = options.'socketType'
        assert socketType
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
            _tidyAndExit(this)
        }

        this
    }

    def replyConnection (connectionAddress = "", Map options = [:], Closure doWork=null) {
        def poolSize = options.'poolSize' ?: DEFAULT_POOL_SIZE
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
            _tidyAndExit(this)
        }

        this
    }

    //pub sub model
    def publisherConnection (connectionAddress = [""], Map options = [:]) {
        def poolSize = options.'poolSize' ?: DEFAULT_POOL_SIZE
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
        def poolSize = options.'poolSize' ?: DEFAULT_POOL_SIZE
        if (!context)
            context = new ZContext(poolSize)

        errors.clear()

        def port = options.'port' ?: "5556"
        def protocol = options.'protocol' ?: 'tcp'
        def host = options.'host' ?: 'localhost'
        if (connectionAddress == [""])
            connectionAddress = ["${protocol}://${host}:${port}"]

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
