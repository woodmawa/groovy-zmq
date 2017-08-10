package org.softwood.scripts

import com.fasterxml.jackson.annotation.JsonRootName
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationConfig
import com.fasterxml.jackson.databind.SerializationFeature
import groovy.transform.ToString

/**
 * Created by willw on 01/07/2017.
 */

ObjectMapper mapper = new ObjectMapper()

//mapper.enable(SerializationFeature.WRAP_ROOT_VALUE)
mapper.enable(SerializationFeature.INDENT_OUTPUT)
//mapper.configure(SerializationConfig/*Feature.WRAP_ROOT_VALUE, true*/);

@JsonTypeInfo(include=JsonTypeInfo.As.WRAPPER_OBJECT, use=JsonTypeInfo.Id.CLASS)
//@JsonRootName(value = "Customer")
@ToString
class Customer {
    String name
    Site site
}

@JsonTypeInfo(include=JsonTypeInfo.As.WRAPPER_OBJECT, use=JsonTypeInfo.Id.CLASS)
@ToString
class Site {
    String sname
}

ArrayList<Customer> custArray = [new Customer(name:"will"), new Customer(name:"maz")]

def enc = mapper.writeValueAsString(custArray)

println "encoded array as json : $enc"

enc =mapper.writeValueAsString(new Customer(name:"dmc", site:new Site(sname:"home")))

println "encoded instance  as json : $enc"

def cust = mapper.readValue (enc, Customer)

println "decoded object $cust"