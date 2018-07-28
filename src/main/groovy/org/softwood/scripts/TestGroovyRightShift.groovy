package org.softwood.scripts

import groovyx.gpars.dataflow.Dataflow
import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.Promise
import org.codehaus.groovy.ast.expr.MethodCall
import org.codehaus.groovy.runtime.MethodClosure

import java.util.function.Function
import java.util.function.Supplier

class Rsh {

    DataflowVariable chainDF

    //chaining method call
    def then (param) {
        this.doWork(param)
    }

    //chaining method call, can only parse 1 param from  >> expression so test for runnable
    def then2 (param) {
        //schedule value return the promise
        chainDF = new DataflowVariable ()
        def runnable = Runnable.isInstance (param)  //runtime check
        if (runnable)
            chainDF << Dataflow.task (param)//schedule the result
        else
            chainDF << param
        this
    }

    private def doWork (param) {

        if (param instanceof Closure ) {
            def res = param.call()
            println "invoked doWork called clos : " + res
        } else
            println "invoked with param  : " + param

        this  //or new Rsh()?

    }

    //can only parse one param as an param on a rightShift expression
    def rightShift (param) {
        then (param)
    }

    /*def rightShift (Closure clos) {
        clos.delegate = this
        clos.call (this)
        this
    }

    def rightShift (Function method) {
        //clos.delegate = this
        method (this)
        this
    }*/
    /*def rightShift (MethodClosure clos) {
        clos.delegate = this
        clos.call (this)
        this
    }*/

    /*def send (Closure sMsg) {
        def buf = sMsg() //get message
        sMsg.delegate = this
        println "sent message : $buf"
        this
    }*/

    def send (msg) {
        def buf
        if (Closure.isInstance (msg)) {
            buf = msg() //invoke and pass return value as message
        } else {//toto msg.responds to evaluate test
            buf = msg
        }
        println "sent message : $buf"
        this
    }

    def getSender() {
        this.&send
    }

    def receive () {
        println "empty receive method called "
        "empty"
    }

    def methodWithOneParam(def a) {
        return "1 param"
    }

    def receive (def rshInstance) {
        def buf = "receive method called, received 'sent message'"
        //rMsg.delegate = this
        //rMsg.call (buf )
        this
    }

    def receive (Closure rMsg) {
        def buf = "received 'sent message'"
        rMsg.delegate = this
        rMsg.call (buf )
        this
    }
    def reply (Closure rrMsg) {
        def buf = "reply to 'sent message'"
        rrMsg.delegate = this
        rrMsg.call (buf)
        this
    }
}

def client = new Rsh()

client >> {"hello"} >> 5
client.then2 {"hello"}
def DataflowVariable p = client.chainDF

println "promise was : " + p.val

def result
MethodClosure method= client.&receive
method()
Supplier sup = client.&receive
def val = sup.get()
Function func = client.&methodWithOneParam
def val2 = func.apply("a") //called fucntion with value
client.send {"hello"} >> method

println "result was : $result"