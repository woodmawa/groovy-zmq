package org.softwood.scripts

import groovy.transform.ToString
import org.codehaus.groovy.runtime.InvokerHelper

import java.util.concurrent.ConcurrentLinkedQueue

import static java.lang.reflect.Modifier.isStatic

import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

@ToString
class SpecialType {
    String name
    BigDecimal sal

    /*def asType (Class clazz) {
        if (clazz == Boolean)
            return true
        else
            return [name, sal]
    }*/
}

@ToString()
class Source {
    String name
    String number
    boolean tOrF
    //static myStat = 1
    double dub
    Map address
    List col
    SpecialType spec
    Building building
}

@ToString
class Target {
    String name
    int  number
    String bar
    String tOrF
    Map address
    Collection col
    Date bday
    List spec
    Building building
}

@ToString
class Building {
    String Name
    BigDecimal height
    Address address
}

@ToString
class Address {
    String number
    String street
    String town
}

interface ValueConverter {
    def convert (value)
    boolean canConvert (value)
    Class<?> getTargetType ()
}

class Binder {

    private static ConcurrentLinkedQueue typeConverters = new ConcurrentLinkedQueue()

    static def registerTypeConverter(sourceType, targetType, converter) {
        assert targetType instanceof Class
        assert sourceType instanceof Class
        typeConverters << [sourceType, targetType, converter]
    }

    //see if standard converter is in registry listing and return it
    static def lookupTypeConvertors(sourceType, targetType) {
        def converters = typeConverters.collect {
            if (it[0] == sourceType && it[1] == targetType)
                it[2]
            else
                null
        }
    }

    //tests if type is simple type classifier or not
    static private boolean isSimpleType(type) {
        ArrayList simpleTypes = [int, short, long, float, char, boolean, double, byte]
        simpleTypes.contains(type)
    }

    /**
     *
     * @param target - either instance of or the class type of the target
     * @param source - source of data to bind
     * @param params
     * @param prefix
     * @return
     */
    static def bind(target, source, Map configureBinderParams = null, prefix = null) {
        boolean isPrimitive

        def targetInstance
        if (target instanceof Class) {
            //create new instance through default factory
            targetInstance = target.newInstance()
        } else
            targetInstance = target

        def sourcePropsList = []
        //find properties that don't contain 'class'
        def targetPropsList = target.metaClass.properties.findAll { !(it.name =~ /.*(C|c)lass.*/) }
        //todo def targetStaticProps = target.metaClass.properties.findAll{ it.getter.static }

        def excludeList = configureBinderParams?.exclude
        def includeList = configureBinderParams?.include

        //if explicit include white list of target properties to bind
        //only build targetProp from exact matches
        if (includeList) {
            def reduced = []

            for (prop in targetPropsList) {
                for (name in includeList) {
                    if (prop.name == name)
                        reduced << prop
                }
            }
            targetPropsList = reduced
        }

        switch (source) {
            case Map :
            case Expando:
                println "source was a map "

                //binding source from list of attributes held in a map, sourceProps is ArrayList of [prop string names, value, type]
                sourcePropsList = source.collect { [it.key, it.value, it.value.getClass()] }

                //if exclusion list remove from source before we process
                if (excludeList) {
                    def reduced = []

                    //only build targetProp from exact matches
                    for (prop in sourcePropsList) {
                        for (name in excludeList) {
                            if (prop[0] != name)
                                reduced << prop
                        }
                    }
                    sourcePropsList = reduced
                }

                def ans = this.processMapAttributes(targetInstance, sourcePropsList)
                break
            case Class:
                println "source was a class instance  "
                sourcePropsList = source.metaClass.properties.findAll { it.name != "class" }

                //if exclusion list remove from source before we process
                if (excludeList) {
                    def reduced = []

                    //only build targetProp from exact matches
                    for (prop in sourcePropsList) {
                        for (name in excludeList) {
                            if (prop.name != name)
                                reduced << prop
                        }
                    }
                    sourcePropsList = reduced
                }

                processSourceInstanceAttributes (targetInstance, sourcePropsList)
                break
            default :
                break
        }

        targetInstance
    }

    /**
     * where source is map bind the leaf property values into the target
     *
     * @param targetInstance - instacne of class being bound from source param map
     * @param paramsList - list of leave properties in form [prop, value, type]
     * @return
     */
    static private def bindLeafMapAttributes(targetInstance, paramsList) {
        assert targetInstance, paramsList

        boolean isPrimitive
        MetaProperty targetProperty

        //for each array entry of [property, value, type], try and bind it in targetInstance
        paramsList?.each { ArrayList prop ->

            def converters
            //check if property exists in the target.
            targetProperty = targetInstance.hasProperty(prop[0])

            //if target doesn't have source match skip to next source property
            if (targetProperty == null)
                return

            if (isSimpleType(targetProperty.type))
                isPrimitive = true
            else
                isPrimitive = false

            if (targetProperty) {
                if (targetProperty.type.isAssignableFrom(prop[2])) {
                    //get value of the source property using closure param metaMethod 'prop as ${prop.getProperty(source)}"
                    //and set this on the target instance
                    println "target prop :'${targetProperty.name}' is assignable from source, set prop value : ${prop[1]}"
                    def value = prop[1]
                    targetProperty.setProperty(targetInstance, value)
                    //targetInstance."${prop[0]}" = value
                } else if (targetProperty.type == String) {
                    println "target prop :'${targetProperty.name}',  casting source to target, set prop value : ${prop[1]} "
                    targetProperty.setProperty(targetInstance, prop[1].toString())
                } else if (isPrimitive) {
                    println "parse to primitive "
                    switch (targetProperty.type) {
                        case int:
                            targetProperty.setProperty(targetInstance, Integer.parseInt(prop[1].toString()))
                            break
                        case char:
                            targetProperty.setProperty(targetInstance, prop[1] as char)
                            break
                        case float:
                            targetProperty.setProperty(targetInstance, Float.parseFloat(prop[1].toString()))
                            break
                        case double:
                            targetProperty.setProperty(targetInstance, Double.parseDouble(prop[1].toString()))
                            break
                        case boolean:
                            targetProperty.setProperty(targetInstance, Boolean.parseBoolean(prop[1].toString()))
                            break
                        default:
                            break

                    }

                } else {
                    converters = typeConverters.collect {
                        if (it[0] == prop[2] && it[1] == targetProperty.type)
                            it[2] as ValueConverter
                        else
                            null
                    }
                    if (converters) {
                        def sourceValue = prop[1]
                        if (sourceValue) {
                            def newValue = converters[0].convert(sourceValue)
                            targetProperty.setProperty(targetInstance, newValue)
                        }
                    }
                }

            }
        }

    }

    /**
     * if the source is a map do the binding from the map attributes, sim0ple and compound
     * @param targetInstance
     * @param params
     * @param result
     * @return
     */
    static private def processMapAttributes(targetInstance, params) {
        def leafParams = params.findAll { !(it[0].contains(".")) }
        def compoundParams = params.findAll { it[0].contains(".") }

        //if any leaf params then bind them
        if (leafParams) {
            bindLeafMapAttributes(targetInstance, leafParams)
        }

        Map attMap = new HashMap()
        List propList = new LinkedList()
        def subParamsBlock = []

        //for each compound property form like [xxx.yyy, value, type]
        //build map of prop name and attributes to bind to that property
        compoundParams.each {
            String tprop
            String[] parts = it[0].tokenize('.')
            if (parts.size() == 2) {
                //process leaf attributes
                tprop = parts[0]
                propList << [parts[1], it[1], it[2]]
                attMap << ["${tprop}": propList]
            }

            if (parts.size() > 2) {
                //if nested attributes set up next param block to process, tack orig target property onto end of list
                tprop = parts[0]
                parts = parts.drop(1)
                subParamsBlock << [parts.join('.'), it[1], it[2], tprop]
            }
        }
        if (attMap) {
            attMap.each { prop, propValue ->
                MetaProperty mprop = targetInstance.hasProperty(prop)
                def subTarget = targetInstance."${prop}"
                if (subTarget == null) {
                    //todo what to do parent child pointer to parent if any ?
                    subTarget = mprop.type.newInstance()
                    mprop.setProperty(targetInstance, subTarget)
                }
                bindLeafMapAttributes(subTarget, propValue)
            }
        }
        println propList

        //if remaining compound attributes, then recurse
        if (subParamsBlock) {
            subParamsBlock.each { subParams ->
                def newSubTargetInstance = targetInstance."${subParams[3]}"
                //process subtarget and reduced subparams block till complete
                def map = processMapAttributes(newSubTargetInstance, subParamsBlock)
            }
        }

        targetInstance
    }

    /**
     * if the source is a map do the binding from the map attributes, sim0ple and compound
     * @param targetInstance
     * @param params
     * @param result
     * @return
     */
    static private def processSourceInstanceAttributes (targetInstance, sourcePropsList) {
        sourcePropsList?.each { MetaProperty prop ->
            def converters
            MetaProperty targetProperty
            boolean isPrimitive

            //if property exists in the target.
            targetProperty = targetInstance.hasProperty("${prop.name}")

            if (targetProperty == null)
                return //return from closure

            if (isSimpleType(targetProperty.type))
                isPrimitive = true
            else
                isPrimitive = false

            if (targetProperty) {
                def sourcePropClassType = prop.type
                println "sourceClassType ${sourcePropClassType.canonicalName}"
                assert sourcePropClassType instanceof Class
                println " tag ${prop.name}, with targetPropclass : ${targetProperty.type}, and sourcePropClass : $sourcePropClassType"

                if (targetProperty.type.isAssignableFrom(sourcePropClassType)) {
                    //get value of the source property using closure param metaMethod 'prop as ${prop.getProperty(source)}"
                    //and set this on the target instance
                    println "target prop is assignable from source, set prop value : ${prop.getProperty(source)}"
                    targetProperty.setProperty(targetInstance, prop.getProperty(source))
                } else if (targetProperty.type == String) {
                    println "casting source to target string type"
                    targetProperty.setProperty(targetInstance, prop.getProperty(source).toString())
                } else if (isPrimitive) {
                    println "parse to primitive "
                    switch (targetProperty.type) {
                        case int:
                            targetProperty.setProperty(targetInstance, Integer.parseInt(prop.getProperty(source).toString()))
                            break
                        case char:
                            targetProperty.setProperty(targetInstance, prop.getProperty(source) as char)
                            break
                        case float:
                            targetProperty.setProperty(targetInstance, Float.parseFloat(prop.getProperty(source).toString()))
                            break
                        case double:
                            targetProperty.setProperty(targetInstance, Double.parseDouble(prop.getProperty(source).toString()))
                            break
                        case boolean:
                            targetProperty.setProperty(targetInstance, Boolean.parseBoolean(prop.getProperty(source).toString()))
                            break
                        default:
                            break

                    }

                } else {
                    //check if any registered custom type convertors
                    converters = typeConverters.collect {
                        if (it[0] == sourcePropClassType && it[1] == targetProperty.type)
                            it[2] as ValueConverter
                        else
                            null
                    }
                    if (converters) {
                        def sourceValue = prop.getProperty(source)
                        if (sourceValue) {
                            def newValue = converters[0].convert(sourceValue)
                            targetProperty.setProperty(targetInstance, newValue)
                        }
                    }
                }


            }

            targetInstance
        }

    }

}

def s = new Source (name:"will", number:"10", tOrF: true, address:[county:'uk',town:'ipswich'], col:["abc",1,true], spec:new SpecialType(name:"woodman", sal: 53.7))
def t = new Target()

Target res
def params = [:]
params << [name:'Will'] << [tOrF: true] << [number:"10"] << [address: [country:"uk", town:"ipswich"]] << [col:["abc,1,true"]] << [bday: new Date()] << [spec: new SpecialType (name:"will", sal: 10.34)]
params << ['building.name':"temple of doom", 'building.height':45.8, 'building.address.street': 'South Close ']

Binder.registerTypeConverter(SpecialType,List, {[it.name, it.sal]} )

res = Binder.bind (Target,params)

println "--> source : " + s
println "--> target databound with : " + res

res = Binder.bind (Target,s, [include: ['tOrF']])
res = Binder.bind (Target, s)

println "++> source : " + s
println "++> target databound with : " + res
