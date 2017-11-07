package org.softwood.scripts

import org.codehaus.groovy.runtime.InvokerHelper

class Source {
    String name
    String number
    boolean tOrF
}

class Target {
    String name
    int number
    String bar
    String tOrF
}

def s = new Source (name:"will", number:"10", tOrF: true)

def t = new Target()

/* gets class cast exception when converting string to int
use (InvokerHelper) {
    t.setProperties (s.properties)
}
*/

def binder (s, t) {
    def sourceProps = s.properties
    sourceProps.remove ('metaClass')
    sourceProps.remove ('class')

    def targetProps = t.properties
    targetProps.remove ('metaClass')
    targetProps.remove ('class')

    sourceProps.each {tag,value ->
        //if attribute exists in the target.
        if (t.'tag') {

        }

    }
    //get map of attribute name and value
    println "targetProps + $targetProps"
    def value = targetProps.tOrF
    println "tOrF value class was " +value.class
}

binder (s,t)

//println "target databound with : " + t.dump()