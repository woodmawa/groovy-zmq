package org.softwood.scripts

import groovy.util.logging.Slf4j
import org.softwood.base.GzmqTrait
import org.zeromq.ZFrame
import org.zeromq.ZMsg

/**
 * Created by willw on 30/06/2017.
 */

class Client3 implements GzmqTrait {

}

@Slf4j
class Server3 implements GzmqTrait {


}

client2 = new Client3 ()
server2 = new Server3 ()

t1 = Thread.start {
    println "start client on new thread "
    //client2.configure("REQ").codec('java').withGzmq {gzmq ->
    client2.withGzmq (socketType:"DEALER") {gzmq ->
        println "client sends message .."
        gzmq.send "Hello Will"
        println "client immediatley sends another message .."
        gzmq.send "So there goes another"

        def reply
        gzmq.receive(String , {reply = it})
        println "client received response : ' ${reply}"

    }
}

t2 = Thread.start {
    println "start server on new thread "
//    server2.codec('json').withGzmq (socketType:"REP") {gzmq ->
    server2.withGzmq (socketType:"ROUTER") {gzmq ->
        //read 1 message from socket
        byte[] request
        gzmq.receive({request = it })
        System.out.println("Server 1st received request as : '${new String (request)}")

        ZMsg clientAddress
        gzmq.receive(null, {request = it }, {caller -> clientAddress = caller })
        System.out.println("Server read 2nd read received request as : '${new String (request)}")

        //echo the message to responder
        String reply = "Server responding with 'Thanks for message' "
        gzmq.reply (reply)


    }
}

t1.join()
t2.join()
println "\t**exiting the script**"