package org.softwood.scripts

import org.softwood.base.GzmqTrait
import org.zeromq.ZMQ

/**
 * Created by willw on 05/07/2017.
 */


class PSClient implements GzmqTrait {
}

class PSServer implements GzmqTrait {
}

class RestOfCode {

}

client = new PSClient ()
client2 = new PSClient ()
client3 = new PSClient ()

server1 = new PSServer ()
server2 = new PSServer ()

def counter = 0
Closure generateDP = { leader ->
    def srandom = new Random ()
    def zipcode = 10000 + srandom.nextInt (10000)
    def temperature = srandom.nextInt (215) - 80 +1
    def humidity = srandom.nextInt (50) + 10 + 1

    def dataPoint = "$leader:${counter++}, $zipcode, $temperature, $humidity"
    //println "generated new datapoint : " + dataPoint
    dataPoint
}




//todo withSocket closes and terminates - on its finally/ reading thread t5 starts but message has gone
//hangs

def t1 = Thread.start {
    server1.withSocket(socketType: "PUB", protocol: "tcp", host: "localhost", port: "5556") { GzmqTrait gzmq ->
        println "pub thread t1 started, server sending 'hello there'"
        ZMQ.Socket  sock =  gzmq.socket
        5.times {
            def data = generateDP("header")
            gzmq.send (data)
        }

        println "server1 messages all sent, thread done "
    }
}

/*
//start subscriber second
def t5 = Thread.start {
    client3.withSocket(socketType: "SUB", protocol: "tcp", host: "localhost", port: "5554") {
        println "sub thread t5 started"
        it.subscribe('') //subscribe all topics
        println "thread 5, called subscribe  on empty string"
        println "thread 5, now calling receive ()"

        def result
        //todo - need to set the subscription
        it.receive(String) { result = it }
        assert !it.hasErrors()
        println "thread 5 client 3 received message : ${result}"
    }
} */

//sleep (1000)

/*
def t2 = Thread.start {
    server2.publisherConnection ()
    if (server2.hasErrors()) {
        println "error: " + server2.errors[0]
    } else {
        //Todo - problem with overwriting timer - maybe better to schedule outside the gmz scope
        //server2.scheduledSend(generateDP("header"), 0, 1000, 5000)
        println "publisher send 5 messages "
        5.times {
            def data = generateDP("header")
            server2.send (data)
        }
    }

    println "stopping publisher server2"
    server2.close()
}
*/

Thread t3 = new Thread ()
t3.start {
    def output
    client.subscriberConnection("header" )
    5.times {
        client.receive { output = it }
        println "**thread ${t3.name} - output received : " + String.newInstance(output)
    }
    println "thread 3 received 5 times - completed"  //never reached timer cancelled before 10 get written
    client.close()
}


Thread t4 = new Thread ()
t4.start {
    def output
    client2.subscriberConnection("header" )  //, ""
    5.times {
        client2.receive { output = it } //(null)
        println "++thread ${t4.name} - output received : " + String.newInstance(output)
    }
    println "thread 5 received 5 times - completed"  //never reached timer cancelled before 10 get written
    client2.close()
}



/*[t1,t2,t3, t4]*.join()*/
//[t1, t2,t3, t4, t5]*.join()
[t1,  t3, t4]*.join()

println "stopping publisher server1"
server1.close()


println "exiting script  ---"

