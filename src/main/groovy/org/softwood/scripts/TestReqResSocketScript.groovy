package org.softwood.scripts

import org.softwood.base.GzmqTrait

/**
 * Created by willw on 30/06/2017.
 */

class Client implements GzmqTrait {

}

class Server implements GzmqTrait {


}

client = new Client ()
server = new Server ()

t1 = Thread.start {
    println "start client on new thread "
    client.codec('json').requestConnection("", [:]) {gzmq ->
        println "client sends message .."
        gzmq.send "Hello Will"

        def reply
        gzmq.receive({reply = it})
        println "client received response : ' ${reply}"

    }.close ()
}

t2 = Thread.start {
    println "start server on new thread "
    server.codec('json').replyConnection("", [complete:true]) {gzmq ->
        //read 1 message from socket
        byte[] request
        gzmq.receive({request = it })
        System.out.println("Server received request as : '${request.toString()}")

        //echo the message to responder
        String reply = "Server responding with 'Thanks for message' "
        gzmq.send (reply)


    }
}

t1.join()
t2.join()
println "\t**exiting the script**"