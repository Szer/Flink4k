# Flink4K

[![CI Master](https://github.com/Szer/Flink4k/workflows/CI%20Master/badge.svg)](https://github.com/Szer/Flink4k/actions?query=workflow%3A%22CI+Master%22)

This library provides an API for Kotlin users of [Flink](https://flink.apache.org/)

The main pain point of using usual Java-based API of Flink is the constant need of passing `TypeInformation`

Scala API solves it with implicits, but you have to create them anyway 

This library leverages reified type ability to preserve type information

So instead of
```kotlin
stream.map(TypeInformation.of(String::class.java)){ it.toString() }
```
You could just write
```kotlin
stream.mapK{ it.toString() } // TypeInformation will be passed implicitly!
```

# Build
- `git clone https://github.com/Szer/Flink4k.git`
- `./gradlew build`
