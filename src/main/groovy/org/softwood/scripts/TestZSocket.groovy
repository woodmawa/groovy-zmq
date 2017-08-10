package org.softwood.scripts

import org.zeromq.ZMQ
import org.zeromq.ZSocket

ZSocket pubZsock = new ZSocket(ZMQ.PUB)
ZSocket subZsock = new ZSocket (ZMQ.SUB)

//suffers from connection lag - subscriber thread picks up first message about message 3

Thread t1 = new Thread ().start {
    println "started subscriber, subscribe 'hello'"
    subZsock.connect("tcp://localhost:5555")
    subZsock.subscribe("")

    ZSocket readyReq = new ZSocket (ZMQ.REQ)
    readyReq.bind("tcp://localhost:5556")
    readyReq.sendStringUtf8("ready for data")
    def readyReqRes =readyReq.receiveStringUtf8()
    println "received '$readyReqRes' from publisher"
    def data = subZsock.receiveStringUtf8()
    println "thread t1 read $data from SUB socket "
}

//sleep (1000)

Thread t2 = new Thread().start {
    println "started publisher"
    pubZsock.bind "tcp://localhost:5555"

    ZSocket startingReply = new ZSocket (ZMQ.REP)
    startingReply.connect("tcp://localhost:5556")
    def subsReady = startingReply.receiveStringUtf8()
    startingReply.sendStringUtf8("here you go fill yer boots ")
    println "received '$subsReady' from subscriber"

    10.times {
        def mess = "hello there $it"
        def res = pubZsock.sendStringUtf8(mess)
        println "$mess"//sleep (20)
    }
    println "thread t2 done - Publisher sent message 10 times"
}



[t1,t2]*.join()

subZsock.close()
pubZsock.close()
println "closed Zsockets, now exit "
