package org.softwood.scripts

import org.nustaq.serialization.FSTConfiguration

FSTConfiguration ser = FSTConfiguration.createDefaultConfiguration()

byte[] bytes = ser.asByteArray("Hello");
println "number of bytes ret is " + bytes.length
System.out.println(new String(bytes,"UTF-8"))

Object deser = ser.asObject(bytes)
println "deserialised object shows as $deser"