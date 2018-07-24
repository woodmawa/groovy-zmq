package org.softwood.scripts

import org.codehaus.groovy.ast.expr.MethodCall
import org.codehaus.groovy.runtime.MethodClosure

import java.util.function.Function
import java.util.function.Supplier

class rsh {

    def rightShift (Closure clos) {
        clos.delegate = this
        clos.call (this)
        this
    }

    def rightShift (Function method) {
        //clos.delegate = this
        method (this)
        this
    }
    /*def rightShift (MethodClosure clos) {
        clos.delegate = this
        clos.call (this)
        this
    }*/

    def send (Closure sMsg) {
        def buf = sMsg() //get message
        sMsg.delegate = this
        println "sent message : $buf"
        this
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

def client = new rsh()

def result
MethodClosure method= client.&receive
method()
Supplier sup = client.&receive
def val = sup.get()
Function func = client.&methodWithOneParam
def val2 = func.apply("a") //called fucntion with value
client.send {"hello"} >> method

println "result was : $result"