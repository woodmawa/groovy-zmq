package org.softwood.scripts

import groovy.transform.ToString
import org.nustaq.serialization.FSTConfiguration

/**
 * Created by willw on 03/07/2017.
 */

@ToString
class FstCustomer implements Serializable{
    String name
    FstSite site
}

@ToString
class FstSite implements Serializable {
    String sname
}

FSTConfiguration ser = FSTConfiguration.createJsonConfiguration(true,false)

ArrayList<Customer> custArray = [new FstCustomer(name:"will", site: new FstSite(sname:"home")),
                                 new FstCustomer(name:"maz")]

byte[] bytes = ser.asByteArray(custArray);
System.out.println(new String(bytes,"UTF-8"))

Object deser = ser.asObject(bytes)
println "deserialised object shows as $deser"